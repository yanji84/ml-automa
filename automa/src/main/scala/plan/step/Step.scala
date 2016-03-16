package com.projectx.automa.plan.step
import com.projectx.automa.plan._
/**
*
* File Name: Step.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
* base class for all steps. step is the smallest unit of action in an automation plan
*/

abstract class Step {
	def check(plan:Plan, executionContext:PlanExecutionContext) : Boolean
}