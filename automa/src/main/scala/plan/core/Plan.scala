package com.projectx.automa.plan

import com.projectx.automa.plan.step.Step
import scala.collection.mutable.ListBuffer

/**
*
* File Name: PrepStage.scala
* Date: Feb 19, 2016
* Author: Ji Yan
*
* representation of an automation plan
*/

class Plan {
	var _root:Option[PlanDAGNode] = None
	var _leafNodes = ListBuffer[PlanDAGNode]()
	def root() = _root.get
	def leafNodes() = _leafNodes.toList

	def addStepNode(newNode:PlanDAGNode, parentNode:PlanDAGNode) : Unit = {
		parentNode.addChild(newNode)
		_leafNodes += newNode
		_leafNodes -= parentNode
	}

	def addStepNode(newNode:PlanDAGNode) : Unit = {
		_root = Some(newNode)
		_leafNodes += newNode
	}

	def serializePlan() : List[List[Step]] = {
		serializePlan(_root.get).toList.map(_.toList)
	}

	private def serializePlan(root:PlanDAGNode) : ListBuffer[ListBuffer[Step]] = {
		var allPlans = ListBuffer[ListBuffer[Step]]()

		if (root.children.length == 0) {
			allPlans(0) += root.step
		} else {
			for (childNode <- root.children) {
				for (onePlan <- serializePlan(childNode)) {
					onePlan.insert(0, root.step)
					allPlans += onePlan
				}
			}
		}
		allPlans
	}
}