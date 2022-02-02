package latis.input

import java.net.URI

import org.scalatest.EitherValues._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.Inside.inside

import latis.data._
import latis.metadata.Metadata
import latis.model._
import latis.time.Time
import latis.util.FileUtils
import latis.util.Identifier.IdentifierStringContext
import latis.util.StreamUtils

class HapiBinaryAdapterSpec extends AnyFlatSpec {

  private lazy val uri: URI = FileUtils.resolvePath("data/hapi_binary_data").get.toUri

  private lazy val reader = new AdaptedDatasetReader {

    def model: DataType = Function.from(
      Time.fromMetadata(Metadata("id" -> "Time", "type" -> "string", "size" -> "24", "units"->"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")).value,
      Tuple.fromElements(
        Scalar(id"Magnitude", DoubleValueType),
        Scalar(id"dBrms", DoubleValueType)
      ).value
    ).value

    def metadata: Metadata = Metadata(id"hapi_binary")

    def adapter: Adapter = HapiBinaryAdapter(model, AdapterConfig(("class","HAPI"), ("id","hapi_binary_data"))).parsingAdapter
  }

  private lazy val ds = reader.read(uri)

  "The first sample in the HAPI Binary dataset" should "contain the correct values" in {
    inside(StreamUtils.unsafeHead(ds.samples)) {
      case Sample(DomainData(Text(time)), RangeData(Real(mag), Real(dbrms))) =>
        time should be ("2019-01-01T00:00:00.000Z")
        mag should be (-1.0E31)
        dbrms should be (-1.0E31)
      case _ => fail("Sample did not contain the expected data.")
    }
  }

}
