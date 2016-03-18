package com.projectx.automa.plan.step
import com.projectx.automa.plan._
/**
*
* File Name: RandomForest.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
* random forest model
*/

class RandomForest extends ModelStep {
	override def check(plan:Plan, executionContext:PlanExecutionContext) : Boolean = {
		return executionContext.columnMap(executionContext.mainDatasetName).filter(columnMap => columnMap("categorical").asInstanceOf[Boolean] && columnMap("label").asInstanceOf[Boolean]).length > 0
	}
}