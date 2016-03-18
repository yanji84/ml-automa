package com.projectx.automa

import breeze.util.BloomFilter
import org.apache.spark.AccumulableParam
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.commons.codec.binary.Base64
import java.util.BitSet
import java.util.UUID
import java.io._

/**
*
* File Name: columnBloomFilter.scala
* Date: Feb 19, 2016
* Author: Ji Yan
*
* Helper class to help build scala code
*/

object codeGen {
	var codeGen = ""
	var stringEncoderIndex = 1
	var featureEncoderIndex = 1

	def getCodeGen() : String = {
		codeGen
	}

	def genImports() : Unit = {
		codeGen = "import org.apache.spark.ml.Pipeline\n"
		codeGen += "import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator\n"
		codeGen += "import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}\n"
		codeGen += "import org.apache.spark.ml.feature.VectorAssembler\n"
		codeGen += "import org.apache.spark.ml.feature.StringIndexer\n"
		codeGen += "import org.apache.spark.ml.feature.OneHotEncoder\n"
		codeGen += "import org.apache.spark.ml.classification.DecisionTreeClassifier\n"
		codeGen += "import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}\n"
		codeGen += "import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}\n"
		codeGen += "import org.apache.spark.ml.classification.MultilayerPerceptronClassifier\n"
		codeGen += "import org.apache.spark.mllib.linalg.{Vector, Vectors}\n"
		codeGen += "import org.apache.spark.sql.Row\n"
		codeGen += "import org.apache.spark.mllib.evaluation.RankingMetrics\n"
		codeGen += "import org.apache.spark.ml.feature.IndexToString\n"
		codeGen += "import org.apache.spark.SparkContext\n"
		codeGen += "import org.apache.spark.SparkContext._\n"
		codeGen += "import org.apache.spark.SparkConf\n"
		codeGen += "import org.apache.spark.sql.SQLContext\n"
		codeGen += "import org.apache.spark.sql.functions._\n"
	}

	def initContext() : Unit = {
		codeGen += "val conf = new SparkConf().setAppName(\"autogenModel\")\n"
		codeGen += "val sc = new SparkContext(conf)\n"
		codeGen += "val sqlContext = new SQLContext(sc)\n"
	}

	def loadDataFrame(name: String, path: String, register: Boolean) : Unit = {
		codeGen += s"""var $name = sqlContext.read.format("com.databricks.spark.csv").option(""" + """"header",""" + """"true").option(""" + """"inferSchema",""" + """"true").load(""" + s""""$path")\n"""
		if (register) {
			codeGen += s"$name.registerTempTable(" + s""""$name")\n"""
		}
	}

	def groupDataFrame(measureFields: String, groupByFields: String, name: String, register: Boolean) : Unit = {
		codeGen += s"""val grouped = sqlContext.sql("select $measureFields $groupByFields from $name group by $groupByFields")\n"""
		if (register) {
			codeGen += "grouped.registerTempTable(" + s""""grouped")\n"""
		}
	}

	def joinDataFrame(mainDsName: String, mainId: String, id:String, cache: Boolean) : Unit = {
		codeGen += s"""var joinedMainDataset = sqlContext.sql("select * from $mainDsName left join grouped on $mainDsName.$mainId = grouped.$id")\n"""
		if (cache) {
			codeGen += "joinedMainDataset.cache\n"
		}
	}

	def dealWithNA() : Unit = {
		codeGen += "joinedMainDataset = joinedMainDataset.na.drop\n"
	}

	def splitDataFrame(trainRatio: Double, testRatio: Double) : Unit = {
		codeGen += s"val Array(trainingData, testData) = joinedMainDataset.randomSplit(Array($trainRatio, $testRatio))\n"
	}

	def encodeStringType(colName: String) : Unit = {
		val colNameIndex = colName + "_index"
		val colNameOnehot = colName + "_onehot"
		codeGen += s"val stringIndexer$stringEncoderIndex = new StringIndexer().setInputCol(" + s""""$colName").setOutputCol(""" + s""""$colNameIndex").setHandleInvalid(""" + """"skip")""" + "\n"
		codeGen += s"val oneHotEncoder$featureEncoderIndex = new OneHotEncoder().setInputCol(" + s""""$colNameIndex").setOutputCol(""" + s""""$colNameOnehot")""" + "\n"
		stringEncoderIndex += 1
		featureEncoderIndex += 1
	}

	def assembleFeatures(mainDatasetColMap:Map[String, Map[String, String]]) : Unit = {
		var assembleFeatures = "val assembler = new VectorAssembler().setInputCols(Array("
		for ((cName, cMap) <- mainDatasetColMap) {
			val cNameOnehot = cName + "_onehot"
			if (cMap("columnType") == "StringType") {
				assembleFeatures += s""""$cNameOnehot","""
			} else {
				assembleFeatures += s""""$cName","""
			}
		}
		assembleFeatures = assembleFeatures.dropRight(1)
		assembleFeatures = assembleFeatures + ")).setOutputCol(" + """"features")""" + "\n"
		codeGen += assembleFeatures
	}

	def buildModel(problemType: String, featureColName: String) : Unit = {
		var modelInstance = ""
		if (problemType == "classification") {
			modelInstance = "val model = new DecisionTreeClassifier().setLabelCol(" + s""""$featureColName")""" + "\n"
		} else if (problemType == "regression") {
			modelInstance = "val model = new GBTRegressor().setLabelCol(" + """"$featureColName")""" + "\n"
		}
		codeGen += modelInstance
	}

	def buildPipeline() : Unit = {
		var pipelineAssembly = "val pipeline = new Pipeline().setStages(Array("
		for (i <- 1 to stringEncoderIndex - 1) {
			pipelineAssembly += s"stringIndexer$i,"
		}

		for (i <- 1 to featureEncoderIndex - 1) {
			pipelineAssembly += s"oneHotEncoder$i,"
		}
		pipelineAssembly += "assembler, model))\n"
		codeGen += pipelineAssembly
	}

	def trainModel() : Unit = {
		codeGen += s"val trainedModel = pipeline.fit(trainingData)\n"
	}

	def writeToFile() : Unit = {
		val fileName = UUID.randomUUID
		val file = new File(fileName.toString)
		val bw = new BufferedWriter(new FileWriter(file))
		bw.write(codeGen)
		bw.close()
	}
}