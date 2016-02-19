package com.projectx.automa

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import scala.util.control.Exception
import scala.util.Try

/**
*
* File Name: generateColumnMeta.scala
* Date: Feb 11, 2016
* Author: Ji Yan
*
* Spark job to generate ML model code in spark
*
*/

object sparkMLCodegen {
	val config = ConfigFactory.load()
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("cleanData")
		val sc = new SparkContext(conf)
		val sqlContext = new SQLContext(sc)

		// manually set the feature column to decide
		val featureCol = "country_destination"
		var featureColType = ""
		var problemType = "regression"

		codeGen.genImports()
		codeGen.initContext()

		val HDFS_URI = config.getString("projectx.backend.filesystem.hdfs")
		val DATASET_DEFAULT_PATH = config.getString("projectx.backend.filesystem.path.dataset")
		val COLUMN_META_DEFAULT_PATH = config.getString("projectx.backend.filesystem.path.column_meta")
		// load each imported dataset
		val fileSystem = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(HDFS_URI), sc.hadoopConfiguration)
		val path = new Path(DATASET_DEFAULT_PATH)
		val datasets = fileSystem.listStatus(path)
		var colMap:Map[String,Array[Map[String, String]]] = Map()
		var mainDatasetColMap:Map[String, Map[String, String]] = Map()
		var mainDataset = "train"
		var mainIdField = ""
		var mainDatasetPath = ""
		for(dataset <- datasets) {
			// heuristic to determine which dataset is the main dataset
			val datasetName = dataset.getPath.toString.split("/").last
			if (datasetName contains "train") {
				mainDataset = datasetName
				mainDatasetPath = dataset.getPath.toString
			} 
			Exception.ignoring(classOf[org.apache.hadoop.mapred.InvalidInputException]) {
				colMap = colMap ++ extractColumnMeta(dataset.getPath().toString, sc, sqlContext, fileSystem)
			}
		}

		for (c <- colMap(mainDataset)) {
			// get main dataset id field ( assuming in multi-dataset case, each data set will contain its own id field )
			if (c("idField") == "true") {
				mainIdField = c("columnName")
			}

			// determine problem type by looking at column type of the y variable
			if (c("columnName") == featureCol) {
				if (c("categorical") == "true") {
					problemType = "classification"
				} else {
					problemType = "regression"
				}
				featureColType = c("columnType")
			}
		}

		/*
		import sqlContext.implicits._
		val colPropDF = sc.parallelize(colMap.toSeq).toDF.withColumnRenamed("_1", "Colume").withColumnRenamed("_2", "Properties")
		val colPropPath = COLUMN_META_DEFAULT_PATH
		Exception.ignoring(classOf[java.io.IOException]) { fileSystem.delete(new Path(colPropPath), true) }
		colPropDF.repartition(1).write.format("json").save(colPropPath)				
		*/

		for (m <- colMap(mainDataset)) {
			mainDatasetColMap += (m("columnName") -> m)
		}

		val mainDatasetName = mainDataset.split('.')(0)
		codeGen.loadDataFrame(mainDatasetName, mainDatasetPath, true)

		if (!colMap.isEmpty && datasets.length > 1) {
			for (d <- datasets) {
				val dsPath = d.getPath.toString
				val dataset = d.getPath.toString.split("/").last
				val dsName = dataset.split('.')(0)

				if (dataset != mainDataset) {
					// initial feature engineering by computing basic statistics on measures
					codeGen.loadDataFrame(dsName, dsPath, true)
					// group by fields are the id field, string type field and categorical field
					var groupByFields: Array[String] = Array[String]()
					// all other fields are the measures
					var measures: Array[String] = Array[String]()
					var idField = ""

					for (m <- colMap(dataset)) {
						if (m("idField") == "true" || m("categorical") == "true" || m("columnType") == "StringType") {
							groupByFields = groupByFields :+ m("columnName")
							mainDatasetColMap += (m("columnName") -> m)
							if (m("idField") == "true") {
								idField = m("columnName")
							}
						} else {
							measures = measures :+ m("columnName")
						}
					}
					var measuresStr = ""
					if (!measures.isEmpty) {
						for (m <- measures) {
							var cAvgMap = Map[String, String]()
							cAvgMap += ("columnName" -> s"avg_$m")
							cAvgMap += ("columnType" -> "DoubleType")
							cAvgMap += ("datasetName" -> dataset)
							mainDatasetColMap += (s"avg_$m" -> cAvgMap)

							var cSumMap = Map[String, String]()
							cAvgMap += ("columnName" -> s"sum_$m")
							cAvgMap += ("columnType" -> "DoubleType")
							cAvgMap += ("datasetName" -> dataset)
							mainDatasetColMap += (s"sum_$m" -> cAvgMap)

							var cMinMap = Map[String, String]()
							cAvgMap += ("columnName" -> s"min_$m")
							cAvgMap += ("columnType" -> "DoubleType")
							cAvgMap += ("datasetName" -> dataset)
							mainDatasetColMap += (s"min_$m" -> cAvgMap)

							var cMaxMap = Map[String, String]()
							cAvgMap += ("columnName" -> s"max_$m")
							cAvgMap += ("columnType" -> "DoubleType")
							cAvgMap += ("datasetName" -> dataset)
							mainDatasetColMap += (s"max_$m" -> cAvgMap)

							measuresStr = measuresStr + s"avg($m) avg_$m, sum($m) sum_$m, min($m) min_$m, max($m) max_$m,"
						}						
					}
					val groupByFieldsStr = groupByFields.mkString(",")
					codeGen.groupDataFrame(measuresStr, groupByFieldsStr, dsName, true)
					codeGen.joinDataFrame(mainDatasetName, mainIdField, idField, true)
				}
			}
		}

		codeGen.dealWithNA()
		codeGen.splitDataFrame(0.7, 0.3)

		// encode all the StringType variables
		for ((cName,cMap) <- mainDatasetColMap) {
			if (cMap("columnType") == "StringType") {
				codeGen.encodeStringType(cName)
			}
		}

		codeGen.assembleFeatures(mainDatasetColMap)

		// decide model type
		var featureColName = ""
		if (featureColType == "StringType") {
			featureColName = featureCol + "_index"
		} else {
			featureColName = featureCol
		}
		codeGen.buildModel(problemType, featureColName)
		codeGen.buildPipeline()
		codeGen.trainModel()
		
		print("codeGen:" + codeGen.getCodeGen)

		//val codeCompiler = new compiler(None)
		//codeCompiler.eval[Unit](codeGen)
	}

	def extractColumnMeta(dataPath:String, sc:SparkContext,sqlContext:SQLContext, fileSystem:org.apache.hadoop.fs.FileSystem) : Map[String,Array[Map[String, String]]] = {
		val CATEGORICAL_COLUMN_THRESHOLD = config.getDouble("projectx.backend.threshold.categorical_column_ratio")
		var colMap:Map[String,Array[Map[String, String]]] = Map()
		val datasetName = dataPath.split("/").last
		var dataset = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(dataPath)
		// cache to speed up per column access later
		dataset.cache
		var columnTypes = dataset.dtypes
		var columns = dataset.columns
		var rowCount = dataset.count + 0.0
		var foundId = false
		for (columnName <- columns) {
			var idField = "false"
			var categorical = "true"
			val columnIndex:Integer = columns.indexOf(columnName)
			val columnType = columnTypes(columnIndex)._2
			// embarassingly simple heuristic to determine if a field is id field
			// just look for the first column whose name ends with id
			if (columnName.toLowerCase.endsWith("id") && !foundId) {
				foundId = true;
				idField = "true";
			}
			Exception.ignoring(classOf[org.apache.spark.sql.AnalysisException]) {
				var propMap = Map[String, String]()
				val colKey = datasetName
				val unique = dataset.select(columnName).distinct.count + 0.0
				// check if column is categorical/nominal
				val uniqueRatio = unique / rowCount
				if (uniqueRatio >= CATEGORICAL_COLUMN_THRESHOLD) {
					categorical = "false"
				}
				propMap += ("categorical" -> categorical,
							"unique" -> unique.toString,
							"uniqueRatio" -> uniqueRatio.toString,
							"columnType" -> columnType,
							"datasetName" -> datasetName,
							"columnName" -> columnName,
							"idField" -> idField)

				if (colMap.contains(colKey)) {
					val newArr = colMap(colKey) :+ propMap
					colMap += (colKey -> newArr)
				} else {
					var colMapArray = Array[Map[String, String]]()
					colMapArray = colMapArray :+ propMap
					colMap += (colKey -> colMapArray)
				}
			}
		}
		return colMap
	}
}