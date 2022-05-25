package latis.util
package hapi

/** Tests for decoding HAPI info responses. */
class InfoDecoderSuite extends JsonDecoderSuite {

  test("decode time coverage and parameters from info response") {
    withJsonResource("data/hapi-info.json") {
      decodedAs[Info](_) {
        case Info(startDate, stopDate, params) =>
          assertEquals(startDate, "1998-02-04T00:00:31Z")
          assertEquals(stopDate, "2019-05-07T23:59:28Z")
          assertEquals(params.size, 2)
      }
    }
  }
}
