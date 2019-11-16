package latis.util
package hapi

/** Tests for decoding HAPI info responses. */
class InfoDecoderSpec extends JsonDecoderSpec {

  "The HAPI info response decoder" should "decode time coverage and parameters" in {
    withJsonResource("data/hapi-info.json") {
      decodedAs(_) { info: Info =>
        inside(info) { case Info(startDate, stopDate, params) =>
          startDate should be ("1998-02-04T00:00:31Z")
          stopDate should be ("2019-05-07T23:59:28Z")
          params should have size 2
        }
      }
    }
  }
}
