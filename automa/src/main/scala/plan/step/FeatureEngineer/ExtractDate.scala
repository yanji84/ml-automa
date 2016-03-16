package com.projectx.automa.plan.step
import com.projectx.automa.plan._
/**
*
* File Name: ExtractDate.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
* extract extra columns from date column
*/

class ExtractDate extends FEStep {
	override def check(plan:Plan, executionContext:PlanExecutionContext) : Boolean = {
		true
	}
}