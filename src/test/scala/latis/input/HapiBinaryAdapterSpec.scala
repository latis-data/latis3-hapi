package latis.input

import java.net.URI

import latis.data.DomainData
import latis.data.RangeData
import latis.data.Real
import latis.data.Sample
import latis.data.Text
import latis.metadata.Metadata
import latis.model._
import latis.util.FileUtils
import latis.util.StreamUtils
import latis.util.Identifier.IdentifierStringContext
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class HapiBinaryAdapterSpec extends FlatSpec {

  val uri: URI = FileUtils.resolvePath("data/hapi_binary_data").get.toUri

  val reader = new AdaptedDatasetReader {
    def model: DataType = Function(
      Scalar(Metadata("id" -> "Time", "type" -> "string", "length" -> "24")),
      Tuple(
        Scalar(Metadata("id" -> "Magnitude", "type" -> "double")),
        Scalar(Metadata("id" -> "dBrms", "type" -> "double"))
      )
    )

    def metadata = Metadata(id"hapi_binary")

    def adapter = new BinaryAdapter(model)
  }

  val ds = reader.read(uri)

  "The first sample in the HAPI Binary dataset" should "contain the correct values" in {
    StreamUtils.unsafeHead(ds.samples) match {
      case Sample(DomainData(Text(time)), RangeData(Real(mag), Real(dbrms))) =>
        time should be ("2019-01-01T00:00:00.000Z")
        mag should be (-1.0E31)
        dbrms should be (-1.0E31)
      case _ => fail("Sample did not contain the expected data.")
    }
  }

}
