package com.projectx.automa.plan.step
import com.projectx.automa.plan._
/**
*
* File Name: ReplaceNull.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
* step to replace null values in each column
*/

class ReplaceNull extends FTStep {
	override def check(plan:Plan, executionContext:PlanExecutionContext) : Boolean = {
		true
	}
}