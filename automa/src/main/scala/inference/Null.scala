package com.projectx.automa.inference
import org.apache.spark.sql.DataFrame
/**
*
* File Name: Null.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
*/

class Null extends Inference {
	override def inferColumn(columnDf:DataFrame, columnMap:Map[String, Any]) : Unit = {
		val noneNullCount = columnDf.filter(columnDf(0).isNotNull()).count + 0.0
		val total = columnDf.count + 0.0
		if (noneNullCount == total) {
			propMap += ("hasNull" -> false)
		} else {
			propMap += ("categorical" -> true)
		}
	}
}