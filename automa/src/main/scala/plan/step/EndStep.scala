package com.projectx.automa.plan.step
import com.projectx.automa.plan._
/**
*
* File Name: EndStep.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
* marks the end of all automation plan
*/

class EndStep extends Step {
	override def check(plan:Plan, executionContext:PlanExecutionContext) : Boolean = {
		true
	}
}