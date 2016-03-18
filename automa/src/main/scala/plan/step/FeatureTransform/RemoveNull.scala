package com.projectx.automa.plan.step
import com.projectx.automa.plan._
import com.projectx.automa.inference._


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
		return executionContext.columnMap(executionContext.mainDatasetName).filter(_(InferenceType.NULL.toString).asInstanceOf[Boolean]).length > 0
	}
}