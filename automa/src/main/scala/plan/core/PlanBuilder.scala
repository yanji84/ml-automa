package com.projectx.automa.plan

import com.projectx.automa.plan.stage._
import com.projectx.automa.plan.strategy._
import com.projectx.automa.plan.step.StartStep
import com.projectx.automa.plan.step.EndStep

/**
*
* File Name: PlanBuilder.scala
* Date: Feb 19, 2016
* Author: Ji Yan
*
* automation plan builder
*/

class PlanBuilder {
	val stages = List[Stage](
		new PrepStage(),
		//new FEStage(),
		new FTStage(),
		new ModelStage()
		//new EnsembleStage()
	)
	def buildPlan(executionContext:PlanExecutionContext) : Plan = {
		val plan = new Plan
		val strategy = new Sequential
		val startStep = new StartStep
		val endStep = new EndStep

		// every plan starts with the start step
		strategy.addStepToPlan(startStep, plan)

		for (stage <- stages) {
			stage.buildPlan(plan, executionContext)
		}

		// every plan ends with the End Step
		strategy.addStepToPlan(endStep, plan)
		plan
	}
}