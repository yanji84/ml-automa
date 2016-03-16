package com.projectx.automa.codeGenerator
import com.projectx.automa.plan._
import com.projectx.automa.plan.step._
/**
*
* File Name: CodeGenerator.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
* base class for all code generators
*/

abstract class CodeGenerator {
	def generateCode(plan:List[Step], context:PlanExecutionContext) : String
}