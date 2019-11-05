package latis.input

import java.net.URI

import latis.metadata.Metadata
import latis.model._
import latis.output.TextWriter
import org.junit.Test
import org.scalatest.junit.JUnitSuite

class TestHapiJsonAdapter extends JUnitSuite {

  @Test
  def testHapiJson = {
    val reader = new AdaptedDatasetReader {
      //def uri: URI = new URI("https://cdaweb.gsfc.nasa.gov/hapi/data?id=AC_H0_MFI&time.min=2019-01-01&time.max=2019-01-02&parameters=Magnitude,dBrms&format=json")
      def uri: URI = new URI(s"file:${System.getProperty("user.home")}/git/latis3-hapi/src/test/resources/data/hapi_json_data.txt")
      def model: DataType = Function(
        Scalar(Metadata("id" -> "Time", "type" -> "string")),
        Tuple(
          Scalar(Metadata("id" -> "Magnitude", "type" -> "float")),
          Scalar(Metadata("id" -> "dBrms", "type" -> "float"))
        )
      )
      def adapter = new HapiJsonAdapter(model)
    }

    val ds = reader.getDataset
    TextWriter().write(ds) //TODO: assert against values in the dataset?
  }
}