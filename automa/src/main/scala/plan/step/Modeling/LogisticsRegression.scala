package com.projectx.automa.plan.step
import com.projectx.automa.plan._
/**
*
* File Name: LogisticsRegression.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
* Logistics Regression model
*/

class LogisticsRegression extends ModelStep {
	override def check(plan:Plan, executionContext:PlanExecutionContext) : Boolean = {
		return executionContext.columnMetaMap(executionContext.mainDatasetName).filter(columnMap => !columnMap("categorical").asInstanceOf[Boolean] && columnMap("label").asInstanceOf[Boolean]).length > 0
	}	
}