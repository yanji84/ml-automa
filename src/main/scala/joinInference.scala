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
* File Name: joinInference.scala
* Date: Oct 28, 2015
* Author: Ji Yan
*
* Spark job to infer joins between datasets
*/

object joinInference {
	val config = ConfigFactory.load()
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("joinInference")
		val sc = new SparkContext(conf)
		val sqlContext = new SQLContext(sc)
		val HDFS_URI = config.getString("projectx.backend.filesystem.hdfs")
		val COLUMN_META_DEFAULT_PATH = config.getString("projectx.backend.filesystem.path.column_meta")
		val COLUMN_JOININFERENCE_PATH = config.getString("projectx.backend.filesystem.path.column_joininference")
		val JOIN_INFERENCE_THRESHOLD = 	config.getDouble("projectx.backend.threshold.join_inference_match_ratio")
		// parameter to force re-generate column join inference for all datasets
		val force = args.length > 0 && args(0) == "force"
		val fileSystem = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(HDFS_URI), sc.hadoopConfiguration)
		val path = new Path(COLUMN_META_DEFAULT_PATH)
		val markerPath = new Path(COLUMN_META_DEFAULT_PATH + "/_done")
		val lastProcessedTime:Long = Try(fileSystem.getFileStatus(markerPath).getModificationTime()) getOrElse 0
		var joinMap:Map[String,Map[String, String]] = Map()
		// infer join on categorical column and normalized columns only
		var allColumnMetaDF = sqlContext.read.json(COLUMN_META_DEFAULT_PATH + "/*/*")
		// 1. infer join for non-integer type categorical column
		// 2. infer join for normalized columns
		allColumnMetaDF = allColumnMetaDF.filter(((allColumnMetaDF("Properties")("inferredType") === "none") &&
												  (allColumnMetaDF("Properties")("columnType") !== "IntegerType") &&
												  (allColumnMetaDF("Properties")("categorical") === "true")) ||
												 (allColumnMetaDF("Properties")("columnName").startsWith("___normalize")))
		allColumnMetaDF = allColumnMetaDF.select("Properties.columnName",
												 "Properties.columnType",
												 "Properties.datasetName",
												 "Properties.uniqueItems",
												 "Properties.bloomFilter",
												 "Properties.bloomFilterNumBuckets",
												 "Properties.bloomFilterNumHashFunctions")
		val datasets = fileSystem.listStatus(path)
		var allDatasets = allColumnMetaDF.select("datasetName").distinct.collect().map(d => d(0).toString).toList
		var inferredDatasets = Array[String]()
		var selectedDatasets = Array[String]()
		for(dataset <- datasets) {
			val datasetLastModifiedTime:Long = dataset.getModificationTime()
			val datasetName = dataset.getPath().toString.split("/").last
			if (force || datasetLastModifiedTime > lastProcessedTime) {
				if (datasetName != "_done") {
					selectedDatasets = selectedDatasets :+ datasetName
				}
			}
		}
		// infer join between columns of each selected dataset and columns of rest datasets
		for (i <- 0 until selectedDatasets.length) {
			val selectedDatasetName = selectedDatasets(i)
			val selectedDatasetColumns = allColumnMetaDF.filter(allColumnMetaDF("datasetName") === selectedDatasetName).select("columnName", "uniqueItems").collect()
			for (selectedDatasetColumn <- selectedDatasetColumns) {
				var selectedColumnSpecialType = "none"
				if (selectedDatasetColumn(0).toString.startsWith("___normalize")) {
					selectedColumnSpecialType = selectedDatasetColumn(0).toString.split("___").last
				}
				for (restDataset <- allDatasets) {
					if (restDataset != selectedDatasetName) {
						val restDatasetColumns = allColumnMetaDF.filter(allColumnMetaDF("datasetName") === restDataset).select("columnName",
																															   "bloomFilter",
																															   "bloomFilterNumBuckets",
																															   "bloomFilterNumHashFunctions").collect()
						for (restDatasetColumn <- restDatasetColumns) {
							var restColumnSpecialType = "none"
							if (restDatasetColumn(0).toString.startsWith("___normalize")) {
								restColumnSpecialType = restDatasetColumn(0).toString.split("___").last
							}
							if (selectedColumnSpecialType == restColumnSpecialType) {
								val colBloomFilter = columnBloomFilter.deserializeBloomFilter(restDatasetColumn(3).toString.toInt,
																							  restDatasetColumn(2).toString.toInt,
																							  restDatasetColumn(1).toString)
								var numMatched = 0.0
								val selectedColumnUniqueItems = selectedDatasetColumn(1).toString.split("\n")
								for (selectedColumnUniqueItem <- selectedColumnUniqueItems) {
									if (colBloomFilter.contains(selectedColumnUniqueItem.drop(1).dropRight(1))) {
										numMatched += 1.0
									}
								}
								val matchRate = numMatched / (selectedColumnUniqueItems.length + 0.0)
								if (matchRate > JOIN_INFERENCE_THRESHOLD) {
									// infer join
									val col1 = selectedDatasetColumn(0)
									val col2 = restDatasetColumn(0)
									var edgeType = "join"
									if (selectedColumnSpecialType == "date") {
										edgeType = "date"
									} else if (selectedColumnSpecialType.startsWith("s2cell")) {
										edgeType = "location"
									}
									val edgeKey = selectedDatasetName + ":" + col1 + "-" + restDataset + ":" + col2 + "-" + edgeType
									joinMap += (edgeKey -> Map("col1" -> col1.toString,
															   "col2" -> col2.toString,
															   "dataset1" -> selectedDatasetName,
															   "dataset2" -> restDataset,
															   "relationship" -> edgeType,
															   "value" -> matchRate.toString,
															   "significant" -> "true",
															   "significance_level" -> JOIN_INFERENCE_THRESHOLD.toString()))

								}
							}
						}
					}
				}
			}
			allDatasets = allDatasets diff List(selectedDatasetName)
		}
		import sqlContext.implicits._
		val convertedDF = sc.parallelize(joinMap.toSeq).toDF.withColumnRenamed("_1", "Relationship").withColumnRenamed("_2", "Properties")
		scala.util.control.Exception.ignoring(classOf[java.io.IOException]) { fileSystem.delete(new org.apache.hadoop.fs.Path(COLUMN_JOININFERENCE_PATH), true) }
		convertedDF.repartition(1).write.format("json").save(COLUMN_JOININFERENCE_PATH)
		// infer join again on updated datasets
		val markerOutputStream = fileSystem.create(markerPath, true);
		markerOutputStream.close()
	}
}