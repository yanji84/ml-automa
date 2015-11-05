package com.projectx.backend

import com.google.common.geometry._
import com.typesafe.config.ConfigFactory
import com.mdimension.jchronic.Chronic
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.Row
import scala.util.Try
import scala.util.matching.Regex

/**
*
* File Name: columnTypeInferenceUdf.scala
* Date: Oct 21, 2015
* Author: Ji Yan
*
* Defines patterns used to infer column types
*
*/

object columnTypeInference {
	val config = ConfigFactory.load()
	// datetime
	val dateTimeFormat = new java.text.SimpleDateFormat("dd-MM-yyyy")
	def findDatetimeColumnHelperFunc(cell:Any) : Boolean = {
		var consecutiveDigits = 0
		val charArray = cell.toString.toArray
		if (charArray.length < 6) { return false }		
		if (charArray.length > 22) { return false }
		for (i <- 0 until charArray.length) {
			if (charArray(i) == '.') { return false }
			if (charArray(i).isDigit) { consecutiveDigits += 1 } else {
				if (consecutiveDigits != 4 &&
					consecutiveDigits != 2 &&
					consecutiveDigits != 1) {
					return false
				}
				consecutiveDigits = 0
			}
		}
		if (consecutiveDigits != 8 &&
			consecutiveDigits != 4 &&
			consecutiveDigits != 2 &&
			consecutiveDigits != 1) {
			return false
		}
		Option(Chronic.parse(cell.toString)).isDefined
	}
	val findDatetimeColumnFunc: (Any => Boolean) = (cell: Any) => {
		Try(findDatetimeColumnHelperFunc(cell)) getOrElse false
	}
	val udfFindDatetimeColumnFunc = udf(findDatetimeColumnFunc)
	val normalizeDatetimeColumnFunc: (Any => String) = (cell: Any) => {
		Try(dateTimeFormat.format(Chronic.parse(cell.toString()).getBeginCalendar().getTime())) getOrElse ""
	}
	val udfNormalizeDatetimeColumnFunc = udf(normalizeDatetimeColumnFunc)

	// location
	val locationPattern = """(\-?\d+(\.\d+)?)°?,\s*(\-?\d+(\.\d+)?)°?""".r
	val findLocationColumnFunc: (Any => Boolean) = (cell: Any) => {
		Try((locationPattern findFirstIn cell.toString).isDefined) getOrElse false
	}
	def normalizeLocationHelperFunc(dataCell:Any) : String = {
		val S2_CELL_LEVEL = config.getLong("projectx.backend.s2_cell_level").toInt
		var cell = S2CellId.fromLatLng(S2LatLng.fromDegrees((locationPattern findFirstIn dataCell.toString).get.split(",")(0).replace("°","").toDouble,
															(locationPattern findFirstIn dataCell.toString).get.split(",")(1).replace("°","").toDouble))
		for (p <- 0 until (30 - S2_CELL_LEVEL)) {
			cell = cell.parent
		}
		cell.toToken
	}
	val udfFindLocationColumnFunc = udf(findLocationColumnFunc)
	val normalizeLocationColumnFunc: (Any => String) = (cell: Any) => {
		Try (normalizeLocationHelperFunc(cell)) getOrElse ""
	}
	val udfNormalizeLocationColumnFunc = udf(normalizeLocationColumnFunc)
}