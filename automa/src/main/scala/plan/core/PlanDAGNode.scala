package com.projectx.automa.plan

import com.projectx.automa.plan.step.Step
import scala.collection.mutable.ListBuffer

/**
*
* File Name: PlanDAGNode.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
* Node in a plan DAG that encapsulates a plan step
*/

case class PlanDAGNode (step:Step) {
	var _children = ListBuffer[PlanDAGNode]()
	def children = _children.toList
	def addChild(c:PlanDAGNode) : Unit = {
		_children += c
	}
}