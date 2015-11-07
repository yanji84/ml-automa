import breeze.util.BloomFilter
import com.projectx.backend.columnBloomFilter
import org.scalatest._

/**
*
* File Name: correlation.scala
* Date: Nov 7, 2015
* Author: Ji Yan
*
* Spec test for columnBloomFilter
*
*/

class columnBloomFilterTest extends FlatSpec with Matchers {
  it should "successfully deserialize and serialize column bloom filter" in {
    val bloomFilter = BloomFilter.optimallySized[String](100, 0.01);
    bloomFilter += "a"
    bloomFilter += "b"
    bloomFilter += "c"
    val serializedBF = columnBloomFilter.serializeBloomFilter(bloomFilter)
    val deserializedBF = columnBloomFilter.deserializeBloomFilter(bloomFilter.numHashFunctions, bloomFilter.numBuckets, serializedBF)
    deserializedBF.contains("d") should be (false)
    deserializedBF.contains("e") should be (false)
    deserializedBF.contains("f") should be (false)

    deserializedBF.contains("a") should be (true)
    deserializedBF.contains("b") should be (true)
    deserializedBF.contains("c") should be (true)
  }
}