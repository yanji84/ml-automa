package com.projectx.automa.inference
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.Map

/**
*
* File Name: Categorical.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
*/

class Categorical extends Inference {
	override def inferColumn(dataDF:DataFrame, columnDf:DataFrame, columnMap:Map[String, Any]) : Unit = {
		val total = columnDf.count + 0.0
		val unique = columnDf.distinct.count + 0.0
		val uniqueRatio = unique / total
		columnMap += (InferenceType.CATEGORICAL.toString -> (1 - uniqueRatio))
	}

	override def inferType() : InferenceType.Value = {
		InferenceType.CATEGORICAL
	}
}