package com.projectx.automa.plan

import com.projectx.automa.plan.step.Step

/**
*
* File Name: PrepStage.scala
* Date: Feb 19, 2016
* Author: Ji Yan
*
* representation of an automation plan
*/

class Plan {
	var _root:PlanDAGNode
	var _leafNodes:List[PlanDAGNode] = List[PlanDAGNode]()
	def root() = _root
	def leafNodes() = _leafNodes

	def addStepNode(newNode:PlanDAGNode, parentNode:PlanDAGNode) : Unit = {
		parentNode.addChild(newNode)
		_leafNodes = (_leafNodes :: newNode).filter(_ != parentNode)
	}

	def addStepNode(newNode:PlanDAGNode) : Unit = {
		_root = newNode
		_leafNodes = _leafNodes :: newNode
	}

	def serializePlan() : List[List[Step]] = {
		serializePlan(_root)
	}

	private def serializePlan(root:PlanDAGNode) : List[List[Step]] = {
		var allPlans:List[List[Step]]

		if (root.children.length == 0) {
			allPlans = new List{new List{root.step}}
		} else {
			for (childNode <- root.children) {
				for (onePlan <- serializePlan(childNode)) {
					allPlans = allPlans :: (root.step :: onePlan)
				}
			}
		}

		allPlans
	}
}