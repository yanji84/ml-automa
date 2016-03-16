package com.projectx.automa.plan.step
import com.projectx.automa.plan._
/**
*
* File Name: Normalize.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
* step to Normalize columns of dataframe
*/

class Normalize extends FTStep {
	override def check(plan:Plan, executionContext:PlanExecutionContext) : Boolean = {
		true
	}
}