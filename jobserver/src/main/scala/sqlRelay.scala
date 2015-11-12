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

/**
*
* File Name: sqlRelay.scala
* Date: Nov 09, 2015
* Author: Ji Yan
*
* Jobserver job execute hive sql query
*/

object sqlRelay extends SparkSqlJob {
	override def runJob(sc:SQLContext, config: Config): Any = {
		val input = config.getString("input.sql").split("%7C")
		val dataset = input(0)
		val sqlStatement = input(1)
		var table = sc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("/projectx/datasets/" + dataset + "/*")
		table.registerTempTable(dataset)
		sc.sql(sqlStatement).collect
	}
	override def validate(sc:SQLContext, config: Config): spark.jobserver.SparkJobValidation = {
		Try(config.getString("input.sql")).map(x => SparkJobValid).getOrElse(SparkJobInvalid("No input.sql config param"))
	}
}
