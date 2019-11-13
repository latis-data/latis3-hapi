package latis.input

import java.net.URI

import latis.data.{Data, Sample, SeqFunction}
import latis.metadata.Metadata
import latis.model._
import latis.output.TextWriter
import latis.util.FileUtils
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class TestHapiBinaryAdapter extends FlatSpec {

  val reader = new AdaptedDatasetReader {
    //def uri: URI = new URI("https://cdaweb.gsfc.nasa.gov/hapi/data?id=AC_H0_MFI&time.min=2019-01-01&time.max=2019-01-02&parameters=Magnitude,dBrms&format=binary")
    //def uri: URI = FileUtils.resolvePath("data/hapi_binary_data").get.toUri
    def uri: URI = new URI(s"file:${System.getProperty("user.home")}/git/latis3-hapi/src/test/resources/data/hapi_binary_data")
    def model: DataType = Function(
      Scalar(Metadata("id" -> "Time", "type" -> "string")),
      Tuple(
        Scalar(Metadata("id" -> "Magnitude", "type" -> "float")),
        Scalar(Metadata("id" -> "dBrms", "type" -> "float"))
      )
    )
    def adapter = new HapiBinaryAdapter(model)
  }

  val ds = reader.getDataset

  "The first sample in the HAPI Binary dataset" should "contain the correct values" in {
    ds.unsafeForce.data match {
      case sf: SeqFunction => sf.samples.head match {
        case Sample(d, r) => (d, r) match {
          case (List(time), List(mag, dbrms)) =>
            time should be (Data.StringValue("2019-01-01T00:00:14.000Z"))
            mag should be (Data.FloatValue(4.566.toFloat))
            dbrms should be (Data.FloatValue(0.293.toFloat))
          case _ => fail("Sample did not contain the expected data.")
        }
      }
      case _ => fail("Could not get samples from the dataset.")
    }
  }

}