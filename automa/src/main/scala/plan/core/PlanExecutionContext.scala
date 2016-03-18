package com.projectx.automa.plan
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext

/**
*
* File Name: PlanExecutionContext.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
*/

case class PlanExecutionContext (val sparkContext:SparkContext,
								 val sqlContext:SQLContext,
								 val label:String,
								 val mainDatasetName:String,
								 columnMap:Map[String,List[Map[String, Any]]],
								 datasetMap:Map[String, String])