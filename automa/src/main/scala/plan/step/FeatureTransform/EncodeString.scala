package com.projectx.automa.plan.step
import com.projectx.automa.plan._
/**
*
* File Name: EncodeString.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
* step to encode string columns in the dataframe
*/

class EncodeString extends FTStep {
	override def check(plan:Plan, executionContext:PlanExecutionContext) : Boolean = {
		return executionContext.columnMetaMap(executionContext.mainDatasetName).filter((columnMap:Map[String, Any]) => columnMap("columnType") == "StringType").length > 0
	}
}