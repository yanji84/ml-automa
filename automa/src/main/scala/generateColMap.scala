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
import com.projectx.automa.inference._
import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer

/**
*
* File Name: GenerateColMap.scala
* Date: Feb 11, 2016
* Author: Ji Yan
*
* Spark job to generate column map ( pre-calculated statistics on per column basis )
*
*/

object GenerateColMap {
	val config = ConfigFactory.load()
	val inferences = List[Inference](new Categorical(),
								     new Null())
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("GenerateColMap")
		val sc = new SparkContext(conf)
		val sqlContext = new SQLContext(sc)

		// load each imported dataset
		val fileSystem = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(config.getString("projectx.backend.filesystem.hdfs")), sc.hadoopConfiguration)
		val path = new Path(config.getString("projectx.backend.filesystem.path.dataset"))
		val datasets = fileSystem.listStatus(path)

		// create column map
		var colMap = Map[String,List[scala.collection.immutable.Map[String, Any]]]()
		for(dataset <- datasets) {
			colMap += (dataset.getPath().toString -> extractColumnMeta(dataset.getPath().toString, sqlContext))
		}

		// persist column map
		val columnMetaPath = new Path(config.getString("projectx.backend.filesystem.path.column_meta") + "/colmap")
		scala.util.control.Exception.ignoring(classOf[java.io.IOException]) { fileSystem.delete(columnMetaPath, true) }
		val markerOutputStream = fileSystem.create(columnMetaPath)
		val oos = new ObjectOutputStream(markerOutputStream)
		// convert mutable map to immutable before persistence
		oos.writeObject(colMap.toMap)
		oos.close()
		markerOutputStream.close()

		// for test purpose, print out the column map ( comment out when testing )
		// val testStr = colMap.map { case (k,v) => (k + ":\n" + v.map(_.map{case (k,v) => (k + ':' + v.toString)}.mkString("\n")).mkString("\n"))}
		// print(testStr.mkString("\n"))
	}

	def extractColumnMeta(dataPath:String, sqlContext:SQLContext) : List[scala.collection.immutable.Map[String,Any]] = {
		var columnMetaList = ListBuffer[scala.collection.immutable.Map[String,Any]]()
		val datasetName = dataPath.split("/").last
		var dataset = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(dataPath)
		// cache to speed up per column access later
		dataset.cache
		var columnTypes = dataset.dtypes
		var columns = dataset.columns
		for (columnName <- columns) {
			var columnMeta = Map[String, Any]()
			val columnIndex:Integer = columns.indexOf(columnName)
			val columnType = columnTypes(columnIndex)._2

			// common column property
			columnMeta += ("columnType" -> columnType,
						   "datasetName" -> datasetName,
						   "columnName" -> columnName)

			// custom inferences to do on every column
			for (inference <- inferences) {
				inference.inferColumn(dataset, dataset.select(columnName), columnMeta)
			}
			columnMetaList += columnMeta.toMap
		}
		columnMetaList.toList
	}
}