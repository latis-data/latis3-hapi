package latis.util
package hapi

/** Tests for decoding HAPI parameters. */
class ParameterDecoderSpec extends JsonDecoderSpec {

  "The HAPI parameter decoder" should "accept scalar parameters" in {
    withJsonResource("data/hapi-parameter-scalar.json") {
      decodedAs(_) { param: Parameter =>
        inside(param) {
          case ScalarParameter(name, tyName, units, length, fill) =>
            name should be ("Np")
            tyName should be ("double")
            units should be ("#/cc")
            length should be (None)
            fill should be (Some("-1.0E31"))
          case _ => fail("Decoded to wrong parameter type.")
        }
      }
    }
  }

  it should "accept vector parameters" in {
    withJsonResource("data/hapi-parameter-vector.json") {
      decodedAs(_) { param: Parameter =>
        inside(param) {
          case VectorParameter(name, tyName, units, length, fill, size) =>
            name should be ("V_GSE")
            tyName should be ("double")
            units should be ("km/s")
            length should be (None)
            fill should be (Some("-1.0E31"))
            size should be (3)
          case _ => fail("Decoded to wrong parameter type.")
        }
      }
    }
  }

  it should "accept parameters with a single bin" in {
    withJsonResource("data/hapi-parameter-array.json") {
      decodedAs(_) { param: Parameter =>
        inside(param) {
          case ArrayParameter(name, tyName, units, length, fill, size, bin) =>
            name should be ("proton_spectrum_uncerts")
            tyName should be ("double")
            units should be ("particles/(sec ster cm^2 keV)")
            length should be (None)
            fill should be (Some("-1e31"))
            size should be (3)

            inside(bin) { case Bin(name, units) =>
              name should be ("energy")
              units should be ("keV")
            }
          case _ => fail("Decoded to wrong parameter type.")
        }
      }
    }
  }

  it should "reject parameters with multiple bins" in {
    withJsonResource("data/hapi-parameter-array-multiple-bins.json") {
      doesNotDecodeAs[Parameter](_)("Failed to reject invalid parameter.")
    }
  }
}
