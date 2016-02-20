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
import java.io.ObjectOutputStream

/**
*
* File Name: generateColMap.scala
* Date: Feb 11, 2016
* Author: Ji Yan
*
* Spark job to generate column map
*
*/

object generateColMap {
	val config = ConfigFactory.load()
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("generateColMap")
		val sc = new SparkContext(conf)
		val sqlContext = new SQLContext(sc)

		val HDFS_URI = config.getString("projectx.backend.filesystem.hdfs")
		val DATASET_DEFAULT_PATH = config.getString("projectx.backend.filesystem.path.dataset")
		val COLUMN_META_DEFAULT_PATH = config.getString("projectx.backend.filesystem.path.column_meta")
		// load each imported dataset
		val fileSystem = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(HDFS_URI), sc.hadoopConfiguration)
		val path = new Path(DATASET_DEFAULT_PATH)
		val datasets = fileSystem.listStatus(path)
		var colMap:Map[String,Array[Map[String, String]]] = Map()
		for(dataset <- datasets) {
			Exception.ignoring(classOf[org.apache.hadoop.mapred.InvalidInputException]) {
				colMap = colMap ++ extractColumnMeta(dataset.getPath().toString, sc, sqlContext, fileSystem)
			}
		}
		scala.util.control.Exception.ignoring(classOf[java.io.IOException]) { fileSystem.delete(new Path(COLUMN_META_DEFAULT_PATH + "/colmap"), true) }
		val markerOutputStream = fileSystem.create(new Path(COLUMN_META_DEFAULT_PATH + "/colmap"))
		val oos = new ObjectOutputStream(markerOutputStream)
		oos.writeObject(colMap)
		oos.close()
		markerOutputStream.close()
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
				foundId = true
				idField = "true"
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