package com.projectx.automa.plan.step
import com.projectx.automa.plan._
/**
*
* File Name: RemoveNull.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
* step to remove null values from the dataframe
*/

class RemoveNull extends FTStep {
	override def check(plan:Plan, executionContext:PlanExecutionContext) : Boolean = {
		return executionContext.columnMetaMap(executionContext.mainDatasetName).filter(_("hasNull").asInstanceOf[Boolean]).length > 0
	}
}