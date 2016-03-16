package com.projectx.automa.plan.step
import com.projectx.automa.plan._
/**
*
* File Name: StartStep.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
* marks the first step in any automation plan
*/

class StartStep extends Step {
	override def check(plan:Plan, executionContext:PlanExecutionContext) : Boolean = {
		true
	}
}