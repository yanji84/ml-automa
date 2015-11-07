package com.projectx.backend

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
* Date: Oct 21, 2015
* Author: Ji Yan
*
* Spark job to extract metadata of each
* column of every dataset available to
* the system
*
*/

object generateColumnMeta {
	val config = ConfigFactory.load()
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("generateColumnMeta")
		val sc = new SparkContext(conf)
		val sqlContext = new SQLContext(sc)
		val HDFS_URI = config.getString("projectx.backend.filesystem.hdfs")
		val DATASET_DEFAULT_PATH = config.getString("projectx.backend.filesystem.path.dataset")
		val COLUMN_META_DEFAULT_PATH = config.getString("projectx.backend.filesystem.path.column_meta")
		// parameter to force re-generate column meta on all datasets
		val force = args.length > 0 && args(0) == "force"
		// load each imported dataset
		val fileSystem = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(HDFS_URI), sc.hadoopConfiguration)
		val path = new Path(DATASET_DEFAULT_PATH)
		val datasets = fileSystem.listStatus(path)
		for(dataset <- datasets) {
			Exception.ignoring(classOf[org.apache.hadoop.mapred.InvalidInputException]) {
				val markerPath = new Path(dataset.getPath() + "/_done")
				val datasetLastModifiedTime:Long = dataset.getModificationTime()
				val datasetLastProcessedTime:Long = Try(fileSystem.getFileStatus(markerPath).getModificationTime()) getOrElse 0
				// generate column meta on newly updated datasets
				if (force || datasetLastModifiedTime > datasetLastProcessedTime) {
					val colMap = extractColumnMeta(dataset.getPath().toString, sc, sqlContext, fileSystem)
					if (!colMap.isEmpty) {
						val datasetName = dataset.getPath().toString().split("/").last
						import sqlContext.implicits._
						val colPropDF = sc.parallelize(colMap.toSeq).toDF.withColumnRenamed("_1", "Colume").withColumnRenamed("_2", "Properties")
						val colPropPath = COLUMN_META_DEFAULT_PATH + "/" + datasetName
						Exception.ignoring(classOf[java.io.IOException]) { fileSystem.delete(new Path(colPropPath), true) }
						colPropDF.repartition(1).write.format("json").save(colPropPath)				
					}
					// process this dataset again when it updates
					val markerOutputStream = fileSystem.create(markerPath, true);
					markerOutputStream.close()
				}
			}
		}
	}

	def extractColumnMeta(path:String, sc:SparkContext,sqlContext:SQLContext, fileSystem:org.apache.hadoop.fs.FileSystem) : Map[String,Map[String, String]] = {
		val NORMALIZED_DATASET_PATH = config.getString("projectx.backend.filesystem.path.normalized_dataset")
		val CATEGORICAL_COLUMN_THRESHOLD = config.getDouble("projectx.backend.threshold.categorical_column_ratio")
		val COLUMN_CORRELATION_DEFAULT_PATH = config.getString("projectx.backend.filesystem.path.column_correlation")
		val SPECIAL_COLUMN_THRESHOLD = config.getDouble("projectx.backend.threshold.columntype_inference_ratio")
		val ENABLE_COLUMN_EXTRACT_META = config.getBoolean("projectx.backend.features.enable_extract_column_meta")
		val ENABLE_CORRELATION_MATRIX = config.getBoolean("projectx.backend.features.enable_correlation_matrix")
		val S2_CELL_LEVEL = config.getLong("projectx.backend.s2_cell_level")
		var colMap:Map[String,Map[String, String]] = Map()
		var correlationColumns = Array[Integer]()
		val datasetName = path.split("/").last
		val dataPath = path + "/data.csv"
		var dataset = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(dataPath)
		var columnTypes = dataset.dtypes
		var columns = dataset.columns
		var normalizedDataset:Option[org.apache.spark.sql.DataFrame] = None
		var rowCount = dataset.count + 0.0
		if (ENABLE_COLUMN_EXTRACT_META) {
			for (columnName <- columns) {
				var specialType = "none"
				Exception.ignoring(classOf[org.apache.spark.sql.AnalysisException]) {
					// infer datetime for each column
					val matched = dataset.filter(columnTypeInference.udfFindDatetimeColumnFunc(col(columnName))).count() + 0.0
					if (matched / rowCount >= SPECIAL_COLUMN_THRESHOLD) {
						specialType = "date"
						val normalizedColumnName = "___normalized" + columnName + "___" + specialType
						normalizedDataset = if (normalizedDataset.isDefined) Some(normalizedDataset.get.withColumn(normalizedColumnName, columnTypeInference.udfNormalizeDatetimeColumnFunc(col(columnName)))) else
																		  	 Some(dataset.withColumn(normalizedColumnName, columnTypeInference.udfNormalizeDatetimeColumnFunc(col(columnName))))
					}
					// infer geolocation for each column
					if (specialType == "none") {
						val matched = dataset.filter(columnTypeInference.udfFindLocationColumnFunc(col(columnName))).count() + 0.0
						if (matched / rowCount >= SPECIAL_COLUMN_THRESHOLD) {
							specialType = "s2cell"
							val normalizedColumnName = "___normalized" + columnName + "___" + specialType + S2_CELL_LEVEL
							normalizedDataset = if (normalizedDataset.isDefined) Some(normalizedDataset.get.withColumn(normalizedColumnName, columnTypeInference.udfNormalizeLocationColumnFunc(col(columnName)))) else
																			  	 Some(dataset.withColumn(normalizedColumnName, columnTypeInference.udfNormalizeLocationColumnFunc(col(columnName))))
						}						
					}
					val colKey = datasetName + "-" + columnName
					colMap += (colKey -> Map("inferredType" -> specialType ))
				}
			}
		}
		val normalizedDataPath = NORMALIZED_DATASET_PATH + "/" + datasetName
		Exception.ignoring(classOf[java.io.IOException]) { fileSystem.delete(new Path(normalizedDataPath), true) }
		if (normalizedDataset.isDefined) {
			// extract column meta for normalized columns
			dataset = normalizedDataset.get
			columnTypes = dataset.dtypes
			columns = dataset.columns
			rowCount = dataset.count + 0.0
			// save normalized table
			normalizedDataset.get.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save(normalizedDataPath)
		}
		if (ENABLE_COLUMN_EXTRACT_META) {
			for (columnName <- columns) {
				var categorical = "true"
				val columnIndex:Integer = columns.indexOf(columnName)
				val columnType = columnTypes(columnIndex)._2
				Exception.ignoring(classOf[org.apache.spark.sql.AnalysisException]) {
					val colKey = datasetName + "-" + columnName
					val columnDF = dataset.select(columnName)
					var propMap = Try(colMap(colKey)) getOrElse Map()
					if (propMap.isEmpty) {
						propMap += ( "inferredType" -> "none" )
					}
					// check if column is categorical/nominal, used for correlation/join inference
					val columnUniqueDF = columnDF.distinct
					val unique = columnUniqueDF.count() + 0.0
					val uniqueRatio = unique / rowCount
					if (uniqueRatio >= CATEGORICAL_COLUMN_THRESHOLD) {
						categorical = "false"
					}
					// build column bloom filter, this is useful to infer join later
					val bloomFilter = columnBloomFilter.buildColumnBloomFilter(columnDF, dataset.count, sc, sqlContext)
					propMap += ("bloomFilter" -> bloomFilter._1,
								"bloomFilterNumHashFunctions" -> bloomFilter._2.toString,
								"bloomFilterNumBuckets" -> bloomFilter._3.toString,
								"categorical" -> categorical,
								"unique" -> unique.toString(),
								"uniqueRatio" -> uniqueRatio.toString(),
								"columnType" -> columnType,
								"datasetName" -> datasetName,
								"columnName" -> columnName)
					if (categorical == "true" || columnName.startsWith("____normalize")) {
						propMap += ("uniqueItems" -> columnUniqueDF.collect.mkString("\n"))
					} else {
						propMap += ("uniqueItems" -> "")						
					}
					colMap += (colKey -> propMap)
				}
				// build correlation matrix only for numeric columns
				if ((columnType == "DoubleType" || columnType == "IntegerType") &&
					(categorical == "false")) {
					correlationColumns = correlationColumns :+ columnIndex
				}
			}
		}
		if (!correlationColumns.isEmpty && ENABLE_CORRELATION_MATRIX) {
			correlation.extract(correlationColumns.toList, dataset, datasetName, fileSystem, sc, sqlContext)
		} else if (correlationColumns.isEmpty) {
			val path = COLUMN_CORRELATION_DEFAULT_PATH + "/" + datasetName
			Exception.ignoring(classOf[java.io.IOException]) { fileSystem.delete(new Path(path), true) }
		}
		return colMap
	}
}