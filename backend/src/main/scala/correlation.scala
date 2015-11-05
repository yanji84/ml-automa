package com.projectx.backend
import com.typesafe.config.ConfigFactory
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
*
* File Name: correlation.scala
* Date: Oct 22, 2015
* Author: Ji Yan
*
* compute column wise correlation
*
*/

object correlation {
	val config = ConfigFactory.load()
	def extract(columnIndices:List[Integer],
				dataset:org.apache.spark.sql.DataFrame,
				datasetName:String,
				fileSystem:org.apache.hadoop.fs.FileSystem,
				sc:SparkContext,
				sqlContext:SQLContext) = {
		val COLUMN_CORRELATION_DEFAULT_PATH = config.getString("projectx.backend.filesystem.path.column_correlation")
		val CORRELATION_SIGNIFICANCE_THRESHOLD = config.getDouble("projectx.backend.threshold.correlation_significance")
		val NA_DEFAULT_ENCODING = config.getDouble("projectx.backend.na_default_encoding")
		var corMap:Map[String,Map[String, String]] = Map()
		val columnTypes = dataset.dtypes
		val rddVector = dataset.map(c => {
			var columnValues = Array[Double]()
			var value = NA_DEFAULT_ENCODING
			for (i <- columnIndices) {
				if (c(i) != null) {
					if (columnTypes(i)._2 == "IntegerType") {
						value = c(i).asInstanceOf[Integer].toDouble
					} else if (columnTypes(i)._2 == "DoubleType") {
						value = c(i).asInstanceOf[Double]
					}
				}
				columnValues = columnValues :+ value
			}
			Vectors.dense(columnValues)
		})
		val correlationMatrix = Statistics.corr(rddVector, "pearson")
		val numRows = correlationMatrix.numRows
		val numCols = correlationMatrix.numCols
		val correlationArray = correlationMatrix.toArray
		val columnNames = dataset.columns.filter(columnName => columnIndices.contains(dataset.columns.indexOf(columnName)))
		for (i <- 0 until correlationArray.length) {
			if (correlationArray(i).toString != "NaN") {
				val col = i / numCols
				val row = i % numRows
				val edgeKey = datasetName + ":" + columnNames(col) + "-" + datasetName + ":" + columnNames(row) + "-correlation"
				val significant = math.abs(correlationArray(i)) > CORRELATION_SIGNIFICANCE_THRESHOLD
				if (row > col && significant) {
					corMap += (edgeKey -> Map("col1" -> columnNames(col),
											  "col2" -> columnNames(row),
											  "dataset1" -> datasetName,
											  "dataset2" -> datasetName,
											  "relationship" -> "correlation",
											  "value" -> correlationArray(i).toString,
											  "significant" -> significant.toString,
											  "significance_level" -> CORRELATION_SIGNIFICANCE_THRESHOLD.toString))
				}				
			}
		}
		import sqlContext.implicits._
		val convertedDF = sc.parallelize(corMap.toSeq).toDF.withColumnRenamed("_1", "Relationship").withColumnRenamed("_2", "Properties")
		val path = COLUMN_CORRELATION_DEFAULT_PATH + "/" + datasetName
		scala.util.control.Exception.ignoring(classOf[java.io.IOException]) { fileSystem.delete(new org.apache.hadoop.fs.Path(path), true) }
		convertedDF.repartition(1).write.format("json").save(path)
	}
}