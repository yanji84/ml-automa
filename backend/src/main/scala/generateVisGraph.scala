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
* File Name: generateVisGraph.scala
* Date: Oct 31, 2015
* Author: Ji Yan
*
* Spark job to generate dataset relationship graph for visualization
*
*/

object generateVisGraph {
	val config = ConfigFactory.load()
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("generateColumnMeta")
		val sc = new SparkContext(conf)
		val sqlContext = new SQLContext(sc)
		val HDFS_URI = config.getString("projectx.backend.filesystem.hdfs")
		val COLUMN_CORRELATION_PATH = config.getString("projectx.backend.filesystem.path.column_correlation")
		val COLUMN_JOIN_PATH = config.getString("projectx.backend.filesystem.path.column_joininference")
		val VIS_GRAPH_PATH = config.getString("projectx.backend.filesystem.path.vis_graph")
		val fileSystem = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(HDFS_URI), sc.hadoopConfiguration)
		val correlationDF = sqlContext.read.json(COLUMN_CORRELATION_PATH + "/*/*")
		val joinDF = sqlContext.read.json(COLUMN_JOIN_PATH + "/*")
		val graphDF = correlationDF.unionAll(joinDF)
		scala.util.control.Exception.ignoring(classOf[java.io.IOException]) { fileSystem.delete(new org.apache.hadoop.fs.Path(VIS_GRAPH_PATH), true) }
		graphDF.repartition(1).write.format("json").save(VIS_GRAPH_PATH)
	}
}