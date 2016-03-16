package com.projectx.automa.plan.step
import com.projectx.automa.plan._
/**
*
* File Name: NoopStep.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
* pass through step
*/

class NoopStep extends Step {
	override def check(plan:Plan, executionContext:PlanExecutionContext) : Boolean = {
		true
	}
}