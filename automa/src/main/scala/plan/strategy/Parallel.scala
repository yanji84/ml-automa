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
*/

class Parallel extends Strategy {
	var leafNodesBeforeAdded:List[PlanDAGNode]
	override def addStepToPlan(step:Step, plan:Plan) : Unit = {
		val newNode = PlanDAGNode(step)
		if (leafNodesBeforeAdded == null) {
			leafNodesBeforeAdded = plan.getLeafNodes
		}
		for (parentNode <- leafNodesBeforeAdded) {
			plan.addStepNode(newNode, parentNode)
		}
	}
}