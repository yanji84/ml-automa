import com.projectx.backend.columnTypeInference
import org.scalatest._

/**
*
* File Name: correlation.scala
* Date: Nov 7, 2015
* Author: Ji Yan
*
* Spec test for columnTypeInference
*
*/

class columnTypeInferenceTest extends FlatSpec with Matchers {
  it should "find datetime in column" in {
    columnTypeInference.findDatetimeColumnFunc("2014-07-01 08:56:00") should be (true)
    columnTypeInference.findDatetimeColumnFunc("11/6/2014") should be (true)
    columnTypeInference.findDatetimeColumnFunc("1/12/2014") should be (true)
    columnTypeInference.findDatetimeColumnFunc("2014-01-01") should be (true)
    columnTypeInference.findDatetimeColumnFunc("2014/01/01") should be (true)
    columnTypeInference.findDatetimeColumnFunc("01/01/2017") should be (true)
  }
  it should "correctly parse datetime in column" in {
    columnTypeInference.normalizeDatetimeColumnFunc("2014-07-01 08:56:00") should be ("01-07-2014")
    columnTypeInference.normalizeDatetimeColumnFunc("11/6/2014") should be ("06-11-2014")
    columnTypeInference.normalizeDatetimeColumnFunc("1/12/2014") should be ("12-01-2014")
    columnTypeInference.normalizeDatetimeColumnFunc("2014-01-01") should be ("01-01-2014")
    columnTypeInference.normalizeDatetimeColumnFunc("2014/01/01 12:01:01") should be ("01-01-2014")
    columnTypeInference.normalizeDatetimeColumnFunc("01/01/2017 12:01:01") should be ("01-01-2017")
  }
  it should "find geolocation in column" in {
    columnTypeInference.findLocationColumnFunc("23.4324, 54.2344") should be (true)
    columnTypeInference.findLocationColumnFunc("-23.4324, 54.2344") should be (true)
    columnTypeInference.findLocationColumnFunc("23.4324, -54.2344") should be (true)
    columnTypeInference.findLocationColumnFunc("23.4324°, -54.2344°") should be (true)
  }
  it should "correctly parse geolocation into s2cell in column" in {
    columnTypeInference.normalizeLocationColumnFunc("23.4324, 54.2344") should be ("3e60d1")
    columnTypeInference.normalizeLocationColumnFunc("23.4324°, -54.2344°") should be ("8b7485")
  }
}