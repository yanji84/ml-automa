package com.projectx.automa.plan.stage

import com.projectx.automa.plan.PlanExecutionContext
import com.projectx.automa.plan.Plan
import com.projectx.automa.plan.strategy._
import com.projectx.automa.plan.step._

/**
*
* File Name: PrepStage.scala
* Date: Feb 19, 2016
* Author: Ji Yan
*
*/

class PrepStage extends Stage {
	val prepSteps = List[PrepStep](/*new JoinMultipleDatasetsStep()*/)
	override def buildPlan(plan:Plan, executionContext:PlanExecutionContext):Unit = {
		val strategy:Sequential = new Sequential
		for (prepStep <- prepSteps) {
			if (prepStep.check(plan, executionContext)) {
				strategy.addStepToPlan(prepStep, plan)
			}
		}
	}
}