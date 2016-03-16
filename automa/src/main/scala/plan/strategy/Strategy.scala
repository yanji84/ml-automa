package com.projectx.automa.plan.strategy

import com.projectx.automa.plan.Plan
import com.projectx.automa.plan.step.Step

/**
*
* File Name: Strategy.scala
* Date: Feb 19, 2016
* Author: Ji Yan
*
* base class for strategy on how to put steps together into a stage plan
*/

abstract class Strategy {
	def addStepToPlan(step:Step, plan:Plan) : Unit
}