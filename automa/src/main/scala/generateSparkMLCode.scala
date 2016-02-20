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
import java.io.ObjectInputStream

/**
*
* File Name: generateSparkMLCode.scala
* Date: Feb 11, 2016
* Author: Ji Yan
*
* job to generate all Spark ML models
*
*/

object generateSparkMLCode {
	val config = ConfigFactory.load()
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("generateSparkMLCode")
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
		val in = fileSystem.open(new Path(COLUMN_META_DEFAULT_PATH + "/colmap"))
		val ois = new ObjectInputStream(in)
		val obj = ois.readObject
		var colMap:Map[String,Array[Map[String, String]]] = obj.asInstanceOf[Map[String,Array[Map[String, String]]]]
		ois.close()
		in.close()
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
		codeGen.writeToFile
		//val codeCompiler = new compiler(None)
		//codeCompiler.eval[Unit](codeGen.getCodeGen)
	}
}