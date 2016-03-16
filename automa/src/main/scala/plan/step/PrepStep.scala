package com.projectx.automa.plan.step
import com.projectx.automa.plan._
/**
*
* File Name: PrepStep.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
*/

class PrepStep extends Step {
	override def check(plan:Plan, executionContext:PlanExecutionContext) : Boolean = {
		true
	}
}