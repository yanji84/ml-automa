package com.projectx.automa

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import java.io.ObjectInputStream
import com.projectx.automa.plan._
import com.projectx.automa.plan.step._
import com.projectx.automa.codeGenerator._

/**
*
* File Name: driver.scala
* Date: Feb 11, 2016
* Author: Ji Yan
*
*/

object Driver {
	def main(args: Array[String]) {
		// initialize spark contexts
		val config = ConfigFactory.load()
		val conf = new SparkConf().setAppName("AutomaDriver")
		val sc = new SparkContext(conf)
		val sqlContext = new SQLContext(sc)

		// make dataset map
		val fileSystem = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(config.getString("projectx.backend.filesystem.hdfs")), sc.hadoopConfiguration)
		val datasetPaths = fileSystem.listStatus(new Path(config.getString("projectx.backend.filesystem.path.dataset"))).map(_.getPath.toString)
		var datasetMap = Map[String, String]()
		for (datasetPath <- datasetPaths) {
			datasetMap += (datasetPath.split('/').last.split('.')(0) -> datasetPath)
		}

		// read column metadata map
		val fileStream = fileSystem.open(new Path(config.getString("projectx.backend.filesystem.path.column_meta") + "/colmap"))
		val objectStream = new ObjectInputStream(fileStream)
		val colMap:Map[String,List[Map[String, Any]]] = objectStream.readObject.asInstanceOf[Map[String,List[Map[String, Any]]]]
		objectStream.close()
		fileStream.close()

		val executionContext = PlanExecutionContext(sc, sqlContext, "country_destination", "train.csv", colMap, datasetMap)
		val planBuilder = new PlanBuilder
		val plan = planBuilder.buildPlan(executionContext)
		val sequentialPlans:List[List[Step]] = plan.serializePlan
		val codeGen:SparkMLCodeGenerator = new SparkMLCodeGenerator
		for (p <- sequentialPlans) {
			val code = codeGen.generateCode(p, executionContext)
		}
	}
}