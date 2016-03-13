package com.projectx.automa.plan.step

/**
*
* File Name: LogisticsRegression.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
* Logistics Regression model
*/

class LogisticsRegression extends ModelStep {
	override def check(plan:Plan, executionContext:ExecutionContext) : Boolean = {
		return executionContext.columnMetaMap[executionContext.mainDatasetName].filter(!_("categorical") && _("label")).count > 0
	}	
}