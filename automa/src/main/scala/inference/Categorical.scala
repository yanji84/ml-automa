package com.projectx.automa.inference
import org.apache.spark.sql.DataFrame

/**
*
* File Name: Categorical.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
*/

class Categorical extends Inference {
	override def inferColumn(columnDf:DataFrame, columnMap:Map[String, Any]) : Unit = {
		var categorical = true
		val total = columnDf.count + 0.0
		val unique = columnDf.distinct.count + 0.0
		// TODO: ( should come up with a probability instead of a binary value )
		val uniqueRatio = unique / rowCount
		if (uniqueRatio >= 0.1) {
			categorical = false
		}
		propMap += ("categorical" -> categorical)
	}
}