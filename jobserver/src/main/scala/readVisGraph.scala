package com.projectx.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
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
	def parseJson(visGraphDF:org.apache.spark.sql.DataFrame): String = {
		val propertiesColumnDF = visGraphDF.select(visGraphDF("Properties"))
		val groups = propertiesColumnDF.map(p => p(0).toString.split(",")(2)).union(propertiesColumnDF.map(p => p(0).toString.split(",")(3))).distinct.collect
		val nodes = propertiesColumnDF.map(p => (p(0).toString.split(",")(0).drop(1), p(0).toString.split(",")(2))).union(propertiesColumnDF.map(p => (p(0).toString.split(",")(1), p(0).toString.split(",")(3)))).distinct.collect
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

		var edgesNode = ""
		val relationships = propertiesColumnDF.map(p => p(0)).collect()
		for (i <- 0 until relationships.length) {
			val relationship = relationships(i).toString.drop(1).dropRight(1).split(",")
			val sourceIndex = nodes.map(n => n._1).indexOf(relationship(0))
			val targetIndex = nodes.map(n => n._1).indexOf(relationship(1))
			val relationshipType = relationship(4)
			val strength = relationship(7)
			var node = "{\"source\":" + sourceIndex + "," + "\"target\":" + targetIndex + "," + "\"type\":\"" + relationshipType + "\",\"value\":" + strength + "}"
			if (i != relationships.length - 1) {
				node += ","
			}
			edgesNode += node
		}
		edgesNode = "\"" + "links" + "\"" + ":" + "[" + edgesNode + "]"
		"{" + groupsNode + "," + nodesNode + "," + edgesNode + "}"
	}
	override def runJob(sc:SparkContext, config: Config): Any = {
		val sqlContext = new SQLContext(sc)
		val visGraphDF = sqlContext.read.json(config.getString("input.visgraph_path") + "/*")
		parseJson(visGraphDF)
	}
	override def validate(sc:SparkContext, config: Config): spark.jobserver.SparkJobValidation = {
		Try(config.getString("input.visgraph_path")).map(x => SparkJobValid).getOrElse(SparkJobInvalid("No input.visgraph_path config param"))
	}
}
