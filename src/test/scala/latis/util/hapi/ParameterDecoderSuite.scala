package latis.util
package hapi

import cats.data.NonEmptyList

/** Tests for decoding HAPI parameters. */
class ParameterDecoderSuite extends JsonDecoderSuite {

  test("accept and decode scalar parameters") {
    withJsonResource("data/hapi-parameter-scalar.json") {
      decodedAs[Parameter](_) {
        case ScalarParameter(name, tyName, units, length, fill) =>
          assertEquals(name, "Np")
          assertEquals(tyName, "double")
          assertEquals(units, Some("#/cc"))
          assertEquals(length, None)
          assertEquals(fill, Some("-1.0E31"))
        case _ => fail("Decoded to wrong parameter type.")
      }
    }
  }

  test("accept and decode vector parameters") {
    withJsonResource("data/hapi-parameter-vector.json") {
      decodedAs[Parameter](_) {
        case VectorParameter(name, tyName, units, length, fill, size) =>
          assertEquals(name, "V_GSE")
          assertEquals(tyName, "double")
          assertEquals(units, Some("km/s"))
          assertEquals(length, None)
          assertEquals(fill, Some("-1.0E31"))
          assertEquals(size, NonEmptyList.of(3,2))
        case _ => fail("Decoded to wrong parameter type.")
      }
    }
  }

  test("accept and decode parameters with a single bin") {
    withJsonResource("data/hapi-parameter-array.json") {
      decodedAs[Parameter](_) {
        case ArrayParameter(name, tyName, units, length, fill, size, NonEmptyList(Bin(binName, binUnits), _)) =>
          assertEquals(name, "proton_spectrum_uncerts")
          assertEquals(tyName, "double")
          assertEquals(units, Some("particles/(sec ster cm^2 keV)"))
          assertEquals(length, None)
          assertEquals(fill, Some("-1e31"))
          assertEquals(size, NonEmptyList.of(3))
          assertEquals(binName, "energy")
          assertEquals(binUnits, "keV")
        case _ => fail("Decoded to wrong parameter type.")
      }
    }
  }

  test("accept and decode parameters with multiple bins") {
    withJsonResource("data/hapi-parameter-array-multiple-bins.json") {
      decodedAs[Parameter](_) {
        case ArrayParameter(_, _, _, _, _, size, bins) =>
          assertEquals(size, NonEmptyList.of(2,3))
          assertEquals(bins, NonEmptyList.of(Bin("x", "m"), Bin("y", "m")))
        case _ => fail("Decoded to wrong parameter type.")
      }
    }
  }

  test("accept and decode parameter with null units") {
    withJsonResource("data/hapi-parameter-null-units.json") {
      decodedAs[Parameter](_) {
        case ScalarParameter(_, _, units, _, _) =>
          assertEquals(units, None)
        case _ => fail("Decoded to wrong parameter type.")
      }
    }
  }
}
