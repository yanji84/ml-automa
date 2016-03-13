package com.projectx.automa.plan.strategy

import com.projectx.automa.plan.Plan
import com.projectx.automa.plan.step.Step
import package com.projectx.automa.plan.PlanDAGNode

/**
*
* File Name: Sequential.scala
* Date: Feb 19, 2016
* Author: Ji Yan
*
*/

class Sequential extends Strategy {
	override def addStepToPlan(step:Step, plan:Plan) : Unit = {
		val newNode = PlanDAGNode(step)
		if (plan.getLeafNodes.length > 0) {
			for (parentNode <- plan.getLeafNodes) {
				plan.addStepNode(newNode, parentNode)
			}			
		} else {
			plan.addStepNode(newNode)
		}
	}
}