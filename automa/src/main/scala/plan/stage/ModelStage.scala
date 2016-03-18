package com.projectx.automa.plan.stage

import com.projectx.automa.plan.PlanExecutionContext
import com.projectx.automa.plan.Plan
import com.projectx.automa.plan.strategy._
import com.projectx.automa.plan.step._

/**
*
* File Name: ModelStage.scala
* Date: Feb 19, 2016
* Author: Ji Yan
*
*/

class ModelStage extends Stage {
	val modelSteps = List[ModelStep](new LogisticsRegression(), new RandomForest())
	override def buildPlan(plan:Plan, executionContext:PlanExecutionContext):Unit = {
		val strategy = new Parallel
		for (modelStep <- modelSteps) {
			if (modelStep.check(plan, executionContext)) {
				strategy.addStepToPlan(modelStep, plan)
			}
		}
	}
}