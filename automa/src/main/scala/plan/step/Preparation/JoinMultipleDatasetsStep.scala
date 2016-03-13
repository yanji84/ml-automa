package com.projectx.automa.plan.step

/**
*
* File Name: JoinMultipleDatasetsStep.scala
* Date: Feb 22, 2016
* Author: Ji Yan
*
*/

/*
class JoinMultipleDatasetsStep extends PrepStep with DFYieldingStep {
	override var dataframe:DataFrame = null
	override var reason:String
	var datasetPathsWithNameMap:Map[String, String]
	var mainDatasetName:String
	var mainDatasetId:String
	override def check(plan:Plan, executionContext:ExecutionContext) : Boolean = {
		val numDatasetsWithId = executionContext.columnMetaMap map {case (datasetName, colMapArray) => (
			var hasId:Boolean = false
			for (a:Map[String, String] <- colMapArray) {
				if (a("idField")) {
					datasetPathsWithNameMap += (a("datasetName") -> a("datasetPath"))
					if (a("mainDataset")) {
						mainDatasetName = a("datasetName")
						mainDatasetId = a("columnName")
					}
					hasId = true
				}
			}
			hasId
		)}.count(true)
		if (numDatasetsWithId > 1) {
			reason = "There are more than one datasets in the input and they can be combined with id field"
			true
		}
		false
	}

	override def deriveDataFrame(plan:Plan, executionContext:ExecutionContext) : DataFrame = {
		if (dataframe == null) {
			var columnMetaMap:Map[String,Array[Map[String, String]]] = executionContext.columnMetaMap
			val sqlContext = executionContext.sqlContext
			// load the dataframes and register sql tables
			for ((datasetName, datasetPath) <- datasetPathsWithNameMap) {
				var dataset = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(datasetPath)
				dataset.registerTempTable(datasetName)
				if (datasetName != mainDatasetName) {
					// group auxilary datasets first
					val groupByFields = columnMetaMap[datasetName].filter(_("idField") || _("categorical") || _("columnType") == "StringType").map(_("columnName"))
					val groupByFieldsStr = groupByFields.mkString(',')
					val measures = columnMetaMap[datasetName].filter(column => !groupByFields.contains(column("columnName"))).map(_("columnName"))
					val measuresStr = measures.map(columnName => s"avg($columnName) avg_$columnName,sum($columnName) sum_$columnName,min($columnName) min_$columnName,max($columnName) max_$columnName").mkString(',')
					val grouped = sqlContext.sql("select $measuresStr $groupByFieldsStr from $datasetName group by $groupByFieldsStr")
					grouped.registerTempTable(datasetName)
					// join datasets
					val datasetId = columnMetaMap(datasetName).filter(_("idField")).map(_("columnName"))
					val joined = sqlContext.sql(s"select * from $mainDatasetName left join $datasetName on $datasetName.$datasetId = $mainDatasetName.$mainDatasetId")
					joined.registerTempTable(mainDatasetName)
				}
			}
			dataframe = joined	
		}
		dataframe
	}
}
*/