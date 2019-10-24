package latis.input

import org.scalatest.FlatSpec
import latis.model.Dataset
import latis.output.TextWriter

class HapiAdapterSpec extends App { //FlatSpec {
  
  //"The HapiAdapter" should "read the sorce_tsi dataset" in {
    val ds = Dataset.fromName("sorce_tsi")
    TextWriter(System.out).write(ds)
  //}
  
}