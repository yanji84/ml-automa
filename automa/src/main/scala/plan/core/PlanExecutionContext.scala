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

case class PlanExecutionContext (sparkContext:SparkContext,
								 sqlContext:SqlContext,
								 label:String,
								 mainDatasetName:String,
								 columnMetaMap:Map[String,Array[Map[String, String]]],
								 datasetMap:Map[String, String])