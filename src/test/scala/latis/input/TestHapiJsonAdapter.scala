package latis.input

import java.net.URI

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.Inside.inside

import latis.data._
import latis.data.DomainData
import latis.dataset.AdaptedDataset
import latis.dsl.ModelParser
import latis.metadata.Metadata
import latis.ops.Selection
import latis.util.dap2.parser.ast._
import latis.util.Identifier.IdentifierStringContext
import latis.util.StreamUtils

class TestHapiJsonAdapter extends AnyFlatSpec {
  
  private lazy val dataset = {
    val model = ModelParser.unsafeParse("time: string -> (Magnitude: double, dBrms: double)")
    //  Function(
    //  Scalar(Metadata("id" -> "Time", "type" -> "string")),
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

  
  "A HapiJsonAdapter" should "read a json response" in {

    val ds = dataset
      .withOperation(Selection(id"time", Gt, "2019-01-01T00:00:01"))
      .withOperation(Selection(id"time", Lt, "2019-01-01T00:00:15"))

    inside(StreamUtils.unsafeHead(ds.samples)) {
      case Sample(DomainData(Text(time)), RangeData(Real(mag), Real(db))) =>
        time should be("2019-01-01T00:00:14.000Z")
        mag should be(4.566)
        db should be(0.293)
    }
  }
  
}
