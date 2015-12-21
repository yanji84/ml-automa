package com.projectx.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.apache.spark.AccumulatorParam
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import scala.util.Try
import spark.jobserver.SparkJob
import spark.jobserver.SparkJobValid
import spark.jobserver.SparkJobInvalid
import java.net.URLDecoder

/**
*
* File Name: readVisGraph.scala
* Date: Nov 01, 2015
* Author: Ji Yan
*
* Jobserver job to read vis graph
*/

object readStreamOutput extends SparkJob {
	override def runJob(sc:SparkContext, config: Config): Any = {
		val output = sc.textFile("/projectx/output/streaming/part-00000")
		output.collect
	}
	override def validate(sc:SparkContext, config: Config): spark.jobserver.SparkJobValidation = SparkJobValid
}
