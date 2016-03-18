package com.projectx.automa.plan.stage

import com.projectx.automa.plan.PlanExecutionContext
import com.projectx.automa.plan.Plan
import com.projectx.automa.plan.strategy._
import com.projectx.automa.plan.step._

/**
*
* File Name: FEStage.scala
* Date: Feb 19, 2016
* Author: Ji Yan
*
* Feature engineering stage
*/

class FEStage extends Stage {
	val featureEngineeringSteps = List[FEStep]()
	override def buildPlan(plan:Plan, executionContext:PlanExecutionContext):Unit = {
		val strategy = new Sequential
		for (featureEngineeringStep <- featureEngineeringSteps) {
			if (featureEngineeringStep.check(plan, executionContext)) {
				strategy.addStepToPlan(featureEngineeringStep, plan)
			}
		}
	}
}