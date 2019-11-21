package latis.input

import java.net.URI

import latis.data._
import latis.metadata.Metadata
import latis.model._
import latis.output.TextWriter
import latis.util.FileUtils
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import latis.util.StreamUtils
import latis.dataset.AdaptedDataset
import latis.ops.Selection
import latis.data.DomainData

class HapiJsonAdapterSpec extends FlatSpec {
  
  val dataset = {
    val model = Function(
      Scalar(Metadata("id" -> "Time", "type" -> "string")),
      Tuple(
        Scalar(Metadata("id" -> "Magnitude", "type" -> "double")),
        Scalar(Metadata("id" -> "dBrms", "type" -> "double"))
      )
    )

    val adapter = new HapiJsonAdapter(
      model,
      new HapiAdapter.Config(
        "id" -> "AC_H0_MFI"
      )
    )

    val baseUri = new URI("https://cdaweb.gsfc.nasa.gov/hapi/")

    new AdaptedDataset(Metadata("AC_H0_MFI"), model, adapter, baseUri)
  }

  
  "A HapiJsonAdapter" should "read a json response" in {

    val ds = dataset
      .withOperation(Selection("time", ">", "2019-01-01T00:00:01"))
      .withOperation(Selection("time", "<", "2019-01-01T00:00:15"))

    StreamUtils.unsafeHead(ds.samples) match {
      case Sample(DomainData(Text(time)), RangeData(Real(mag), Real(db))) =>
        time should be("2019-01-01T00:00:14.000Z")
        mag should be(4.566)
        db should be(0.293)
    }
  }
  
}
