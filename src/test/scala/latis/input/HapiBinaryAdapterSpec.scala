package latis.input

import java.net.URI

import latis.data.{DomainData, RangeData, Real, Sample, Text}
import latis.metadata.Metadata
import latis.model._
import latis.util.{FileUtils, StreamUtils}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class HapiBinaryAdapterSpec extends FlatSpec {

  val reader = new AdaptedDatasetReader {
    //def uri: URI = new URI("https://cdaweb.gsfc.nasa.gov/hapi/data?id=AC_H0_MFI&time.min=2019-01-01&time.max=2019-01-02&parameters=Magnitude,dBrms&format=binary")
    def uri: URI = FileUtils.resolvePath("data/hapi_binary_data").get.toUri
    def model: DataType = Function(
      Scalar(Metadata("id" -> "Time", "type" -> "string", "length" -> "24")),
      Tuple(
        Scalar(Metadata("id" -> "Magnitude", "type" -> "double")),
        Scalar(Metadata("id" -> "dBrms", "type" -> "double"))
      )
    )
    def adapter = new BinaryAdapter(model)
  }

  val ds = reader.getDataset

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