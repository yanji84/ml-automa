package com.projectx.automa.plan.step
import com.projectx.automa.plan._
/**
*
* File Name: EnsembleStep.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
*/

abstract class EnsembleStep extends Step {
	override def check(plan:Plan, executionContext:PlanExecutionContext) : Boolean
}