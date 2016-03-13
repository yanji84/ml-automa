package com.projectx.automa.plan.step

/**
*
* File Name: RemoveNull.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
* step to remove null values from the dataframe
*/

class RemoveNull extends FTStep {
	override def check(plan:Plan, executionContext:ExecutionContext) : Boolean = {
		return executionContext.columnMetaMap[executionContext.mainDatasetName].filter(_("containsNull")).count > 0
	}
}