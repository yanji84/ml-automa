package com.projectx.automa.plan.step
import com.projectx.automa.plan._
/**
*
* File Name: FTStep.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
* base class for all feature transformation step
*/

class FTStep extends Step {
	override def check(plan:Plan, executionContext:PlanExecutionContext) : Boolean = {
		true
	}
}