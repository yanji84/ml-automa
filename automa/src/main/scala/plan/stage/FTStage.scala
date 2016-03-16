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
* Feature Transformation stage
*/

class FTStage extends Stage {
	val featureTransformSteps = List[FTStep](new RemoveNull(), new EncodeString())
	override def buildPlan(plan:Plan, executionContext:PlanExecutionContext):Unit = {
		val strategy = new Sequential

		for (ftStep <- featureTransformSteps) {
			if (ftStep.check(plan, executionContext)) {
				strategy.addStepToPlan(ftStep, plan)
			}
		}
	}
}