package com.projectx.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.apache.spark.AccumulatorParam
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import scala.util.Try
import spark.jobserver.SparkJob
import spark.jobserver.SparkHiveJob
import spark.jobserver.SparkSqlJob
import spark.jobserver.SparkJobValid
import spark.jobserver.SparkJobInvalid
import java.net.URLDecoder
import java.io.ObjectInputStream
import org.apache.hadoop.fs.Path

/**
*
* File Name: readColMap.scala
* Date: Jan 31, 2016
* Author: Ji Yan
*
* Jobserver job to read colMap
*/

object readColMap extends SparkJob {
	override def runJob(sc:SparkContext, config: Config): Any = {
		val fileSystem = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://158.85.79.185:9000"), sc.hadoopConfiguration)
		val input = URLDecoder.decode(URLDecoder.decode(config.getString("input")))
		val path = "/projectx/datasets/" + input + "_out"
		val in = fileSystem.open(new Path(path + "/colmap"))
		val ois = new ObjectInputStream(in)
		val obj = ois.readObject
		var colMap:Map[String,Array[Map[String, String]]] = obj.asInstanceOf[Map[String,Array[Map[String, String]]]]
		ois.close()
		in.close()
		println(scala.util.parsing.json.JSONObject(colMap))
	}
	override def validate(sc:SparkContext, config: Config): spark.jobserver.SparkJobValidation = {
		Try(config.getString("input")).map(x => SparkJobValid).getOrElse(SparkJobInvalid("No input config param"))
	}
}
