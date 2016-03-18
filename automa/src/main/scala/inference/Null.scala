package com.projectx.automa.inference
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.Map

/**
*
* File Name: Null.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
*/

class Null extends Inference {
	override def inferColumn(dataDF:DataFrame, columnDf:DataFrame, columnMap:Map[String, Any]) : Unit = {
		val noneNullCount = columnDf.filter(columnDf(columnDf.columns(0)).isNotNull).count + 0.0
		val total = columnDf.count + 0.0
		if (noneNullCount == total) {
			columnMap += (InferenceType.NULL.toString -> false)
		} else {
			columnMap += (InferenceType.NULL.toString -> true)
		}
	}

	override def inferType() : InferenceType.Value = {
		InferenceType.NULL
	}
}