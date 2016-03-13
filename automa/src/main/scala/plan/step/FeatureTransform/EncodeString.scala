package com.projectx.automa.plan.step

/**
*
* File Name: EncodeString.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
* step to encode string columns in the dataframe
*/

class EncodeString extends FTStep {
	override def check(plan:Plan, executionContext:ExecutionContext) : Boolean = {
		return executionContext.columnMetaMap[executionContext.mainDatasetName].filter(_("columnType") == "StringType").count > 0
	}
}