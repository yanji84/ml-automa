package com.projectx.automa.inference

import org.apache.spark.sql.DataFrame
import scala.collection.mutable.Map

/**
*
* File Name: Inference.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
* base class for all column inferences
*/

object InferenceType extends Enumeration {
	type InferenceType = Value
	val CATEGORICAL = Value("CATEGORICAL")
	val NULL = Value("NULL")
	// add new column inference methods here
}

abstract class Inference {
	def inferColumn(dataDF:DataFrame, columnDf:DataFrame, columnMap:Map[String, Any])
	def inferType() : InferenceType.Value
}