package com.projectx.backend

import breeze.util.BloomFilter
import org.apache.spark.AccumulableParam
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.commons.codec.binary.Base64
import java.util.BitSet

/**
*
* File Name: columnBloomFilter.scala
* Date: Oct 25, 2015
* Author: Ji Yan
*
* Helper class to build bloom filter for each column
*/

object columnBloomFilter {
	def buildColumnBloomFilter(dataset:org.apache.spark.sql.DataFrame, expectedInsertionNum:Long, sc:SparkContext,sqlContext:SQLContext) : (String, Integer, Integer) = {
		def bloomAccumulableParam = new AccumulableParam[BloomFilter[String], String] {
			def addInPlace(b1: BloomFilter[String], b2: BloomFilter[String]):BloomFilter[String] = {
				b1 |= b2
			}
			def addAccumulator(bloom: BloomFilter[String], value: String) : BloomFilter[String] = {
				bloom += value.toLowerCase
			}
			def zero(bloom: BloomFilter[String]) : BloomFilter[String] = {
				bloom
			}
		}
		val bloomFilter = BloomFilter.optimallySized[String](expectedInsertionNum, 0.01);
		val bloomAccumulable = sc.accumulable(bloomFilter)(bloomAccumulableParam)
		dataset.foreach(r => {
			if (r(0) != null) {
				bloomAccumulable += r(0).toString()
			}
		})
		(serializeBloomFilter(bloomFilter), bloomFilter.numHashFunctions, bloomFilter.numBuckets)
	}
	def serializeBloomFilter(bloomFilter:BloomFilter[String]) : String = {
		Base64.encodeBase64String(bloomFilter.bits.toByteArray())
	}
	def deserializeBloomFilter(numHashFunctions:Integer, numBuckets:Integer, stringifiedBits:String) : BloomFilter[String] = {
		new BloomFilter[String](numBuckets, numHashFunctions, BitSet.valueOf(Base64.decodeBase64(stringifiedBits)))
	}
}