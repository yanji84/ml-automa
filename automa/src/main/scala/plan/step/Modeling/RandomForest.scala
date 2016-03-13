package com.projectx.automa.plan.step

/**
*
* File Name: RandomForest.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
* random forest model
*/

class RandomForest extends ModelStep {
	override def check(plan:Plan, executionContext:ExecutionContext) : Boolean = {
		return executionContext.columnMetaMap[executionContext.mainDatasetName].filter(_("categorical") && _("label")).count > 0
	}
}