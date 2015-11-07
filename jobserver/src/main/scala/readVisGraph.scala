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

/**
*
* File Name: readVisGraph.scala
* Date: Nov 01, 2015
* Author: Ji Yan
*
* Jobserver job to read vis graph
*/

object readVisGraph extends SparkJob {
	def parseJson(visGraphDF:org.apache.spark.sql.DataFrame, sc:SparkContext): String = {
		val groups = visGraphDF.select("dataset1").unionAll(visGraphDF.select("dataset2")).distinct.map(g => g(0)).collect
		val nodes = visGraphDF.select(visGraphDF("col1"), visGraphDF("dataset1")).unionAll(visGraphDF.select(visGraphDF("col2"), visGraphDF("dataset2"))).distinct.map(n => (n(0), n(1))).collect
		var groupsNode = ""
		for (i <- 0 until groups.length) {
			var node = "{\"name\":\"" + groups(i) + "\"}"
			if (i != groups.length - 1) {
				node += ","
			}
			groupsNode += node
		}
		groupsNode = "\"" + "groups" + "\"" + ":" + "[" + groupsNode + "]"
		var nodesNode = ""
		for (i <- 0 until nodes.length) {
			var node = "{" + "\"name\":\"" + nodes(i)._1 + "\",\"group\":" + groups.indexOf(nodes(i)._2) + "}"
			if (i != nodes.length - 1) {
				node += ","
			}
			nodesNode += node
		}
		nodesNode = "\"" + "nodes" + "\"" + ":" + "[" + nodesNode + "]"
		object StringAccumulatorParam extends AccumulatorParam[String] {
			def zero(initialValue: String): String = { "" }
			def addInPlace(s1: String, s2: String): String = { s1 + s2 }
		}
		val columns = visGraphDF.columns
		var edgesNode = sc.accumulator("")(StringAccumulatorParam)
		visGraphDF.foreach(relationship => {
			val sourceIndex = nodes.map(n => n._1).indexOf(relationship(columns.indexOf("col1")))
			val targetIndex = nodes.map(n => n._1).indexOf(relationship(columns.indexOf("col2")))
			val relationshipType = relationship(columns.indexOf("relationship"))
			val strength = relationship(columns.indexOf("value"))
			var node = "{\"source\":" + sourceIndex + "," + "\"target\":" + targetIndex + "," + "\"type\":\"" + relationshipType + "\",\"value\":" + strength + "}"
			node += ","
			edgesNode += node
		})
		"{" + groupsNode + "," + nodesNode + "," + "\"" + "links" + "\"" + ":" + "[" + edgesNode.value.dropRight(1) + "]" + "}"
	}
	override def runJob(sc:SparkContext, config: Config): Any = {
		val sqlContext = new SQLContext(sc)
		val visGraphDF = sqlContext.read.json(config.getString("input.visgraph_path") + "/*")
		parseJson(visGraphDF, sc)
	}
	override def validate(sc:SparkContext, config: Config): spark.jobserver.SparkJobValidation = {
		Try(config.getString("input.visgraph_path")).map(x => SparkJobValid).getOrElse(SparkJobInvalid("No input.visgraph_path config param"))
	}
}
