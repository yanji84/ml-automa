package com.projectx.automa.plan.step
import com.projectx.automa.plan._
/**
*
* File Name: ModelStep.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
* base class for all modeling steps
*/

class ModelStep extends Step {
	override def check(plan:Plan, executionContext:PlanExecutionContext) : Boolean = {
		true
	}
}