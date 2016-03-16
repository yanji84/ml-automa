package com.projectx.automa.plan.stage

import com.projectx.automa.plan.PlanExecutionContext
import com.projectx.automa.plan.Plan

/**
*
* File Name: Stage.scala
* Date: Feb 19, 2016
* Author: Ji Yan
*
*/

abstract class Stage {
	def buildPlan(plan:Plan, executionContext:PlanExecutionContext)
}