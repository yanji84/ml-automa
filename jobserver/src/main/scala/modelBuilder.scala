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

/**
*
* File Name: modelBuilder.scala
* Date: Jan 31, 2016
* Author: Ji Yan
*
* Jobserver job building ml models
*/

object modelBuilder extends SparkSqlJob {
	override def runJob(sc:SQLContext, config: Config): Any = {
		var table = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("/projectx/datasets/airbnb/*")
	}
	override def validate(sc:SQLContext, config: Config): spark.jobserver.SparkJobValidation = {
		SparkJobValid
	}
}
