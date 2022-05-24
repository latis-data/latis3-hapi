package latis.input

import java.net.URI

import munit.CatsEffectSuite

import latis.data._
import latis.dataset.AdaptedDataset
import latis.dsl.ModelParser
import latis.metadata.Metadata
import latis.ops.Selection
import latis.util.Identifier._
import latis.util.dap2.parser.ast._

class HapiJsonAdapterSuite extends CatsEffectSuite {

  private lazy val dataset = {
    val model = ModelParser.unsafeParse("time: string -> (Magnitude: double, dBrms: double)")
    //  Function(
    //  Scalar(Metadata("id" -> "time", "type" -> "string")),
    //  Tuple(
    //    Scalar(Metadata("id" -> "Magnitude", "type" -> "double")),
    //    Scalar(Metadata("id" -> "dBrms", "type" -> "double"))
    //  )
    //)

    val adapter = new HapiJsonAdapter(
      model,
      new HapiAdapter.Config(
        "id" -> "AC_H0_MFI"
      )
    )

    val baseUri = new URI("https://cdaweb.gsfc.nasa.gov/hapi/")

    new AdaptedDataset(Metadata(id"AC_H0_MFI"), model, adapter, baseUri)
  }

  test("read the first sample of a HAPI json dataset with time selection") {

    val ds = dataset
      .withOperation(Selection(id"time", Gt, "2019-01-01T00:00:01"))
      .withOperation(Selection(id"time", Lt, "2019-01-01T00:00:15"))

    ds.samples.compile.toList.map {
      case Sample(DomainData(Text(time)), RangeData(Real(mag), Real(db))) :: _ =>
        assertEquals(time, "2019-01-01T00:00:14.000Z")
        assertEquals(mag, 4.566)
        assertEquals(db, 0.293)
      case _ => fail("Sample did not contain the expected data")
    }
  }
}
