package com.projectx.automa.plan.step
import com.projectx.automa.plan._
/**
*
* File Name: FEStep.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
* base class for all feature engineering steps
*/

class FEStep extends Step {
	override def check(plan:Plan, executionContext:PlanExecutionContext) : Boolean = {
		true
	}
}