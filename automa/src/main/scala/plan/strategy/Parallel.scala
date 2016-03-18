package com.projectx.automa.plan.strategy

import com.projectx.automa.plan.Plan
import com.projectx.automa.plan.step.Step
import com.projectx.automa.plan.PlanDAGNode

/**
*
* File Name: Parallel.scala
* Date: Feb 19, 2016
* Author: Ji Yan
*
* Parallel strategy adds step to plan as sibling of one another
*
*/

class Parallel extends Strategy {
	var leafNodesBeforeAdded:Option[List[PlanDAGNode]] = None
	override def addStepToPlan(step:Step, plan:Plan) : Unit = {
		val newNode = PlanDAGNode(step)
		if (leafNodesBeforeAdded.isEmpty) {
			leafNodesBeforeAdded = Some(plan.leafNodes)
		}
		if (leafNodesBeforeAdded.get.length > 0) {
			for (parentNode <- leafNodesBeforeAdded.get) {
				plan.addStepNode(newNode, parentNode)
			}
		} else {
			plan.addStepNode(newNode)
		}
	}
}