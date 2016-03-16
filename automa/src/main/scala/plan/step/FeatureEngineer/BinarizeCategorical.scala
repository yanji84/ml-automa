package com.projectx.automa.plan.step
import com.projectx.automa.plan._
/**
*
* File Name: BinarizeCategorical.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
* extract binary features from categorical feature
*/

class BinarizeCategorical extends FEStep {
	override def check(plan:Plan, executionContext:PlanExecutionContext) : Boolean = {
		true
	}
}