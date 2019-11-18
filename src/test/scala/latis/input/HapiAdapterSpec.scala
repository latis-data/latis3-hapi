package latis.input

import java.net.URI

import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import latis.data.DomainData
import latis.data.RangeData
import latis.data.Real
import latis.data.Sample
import latis.data.Text
import latis.dataset.AdaptedDataset
import latis.metadata.Metadata
import latis.model.Function
import latis.model.Scalar
import latis.ops.Selection
import latis.util.StreamUtils

object HapiAdapterSpec extends FlatSpec {

  val dataset = {
    val model = Function(
      Scalar(Metadata("id" -> "time", "type" -> "string")),
      Scalar(Metadata("id" -> "tsi_1au", "type" -> "double"))
    )

    val adapter = new HapiCsvAdapter(
      model,
      new HapiAdapter.Config(
        "class" -> "latis.input.HapiCsvAdapter",
        "id" -> "sorce_tsi_24hr_l3"
      )
    )

    val baseUri = new URI("http://lasp.colorado.edu/lisird/hapi/")

    new AdaptedDataset(Metadata("sorce_tsi"), model, adapter, baseUri)
  }
//  val dataset = Dataset.fromName("sorce_tsi") //get from fdml

  "A hapi csv request with time selections" should "return csv records" in {
    val ds = dataset
      .withOperation(Selection("time", ">", "2010-01-01"))
      .withOperation(Selection("time", "<", "2011-01-01"))

    StreamUtils.unsafeHead(ds.samples) match {
      case Sample(DomainData(Text(time)), RangeData(Real(tsi))) =>
        time should be("2010-01-01T12:00:00.000Z")
        tsi should be(1360.4905)
    }
  }

}
