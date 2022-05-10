package latis.util
package hapi

/** Tests for decoding HAPI parameters. */
class ParameterDecoderSuite extends JsonDecoderSuite {

  test("accept and decode scalar parameters") {
    withJsonResource("data/hapi-parameter-scalar.json") {
      decodedAs(_) { param: Parameter =>
        param match {
          case ScalarParameter(name, tyName, units, length, fill) =>
            assertEquals(name, "Np")
            assertEquals(tyName, "double")
            assertEquals(units, "#/cc")
            assertEquals(length, None)
            assertEquals(fill, Some("-1.0E31"))
          case _ => fail("Decoded to wrong parameter type.")
        }
      }
    }
  }

  test("accept and decode vector parameters") {
    withJsonResource("data/hapi-parameter-vector.json") {
      decodedAs(_) { param: Parameter =>
        param match {
          case VectorParameter(name, tyName, units, length, fill, size) =>
            assertEquals(name, "V_GSE")
            assertEquals(tyName, "double")
            assertEquals(units, "km/s")
            assertEquals(length, None)
            assertEquals(fill, Some("-1.0E31"))
            assertEquals(size, 3)
          case _ => fail("Decoded to wrong parameter type.")
        }
      }
    }
  }

  test("accept and decode parameters with a single bin") {
    withJsonResource("data/hapi-parameter-array.json") {
      decodedAs(_) { param: Parameter =>
        param match {
          case ArrayParameter(name, tyName, units, length, fill, size, bin) =>
            assertEquals(name, "proton_spectrum_uncerts")
            assertEquals(tyName, "double")
            assertEquals(units, "particles/(sec ster cm^2 keV)")
            assertEquals(length, None)
            assertEquals(fill, Some("-1e31"))
            assertEquals(size, 3)

            bin match {
              case Bin(name, units) =>
                assertEquals(name, "energy")
                assertEquals(units, "keV")
              case _ => fail("Decoded bin incorrectly")
            }
          case _ => fail("Decoded to wrong parameter type.")
        }
      }
    }
  }

  test("reject parameters with multiple bins") {
    withJsonResource("data/hapi-parameter-array-multiple-bins.json") {
      doesNotDecodeAs[Parameter](_)("Failed to reject invalid parameter.")
    }
  }
}
