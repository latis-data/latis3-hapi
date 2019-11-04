package latis.input

import latis.metadata.Metadata
import latis.model._
import latis.output.TextWriter

import java.net.URI

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
      //val config = new TextAdapter.Config(("dataMarker", "\"data\":["))
      val config = new TextAdapter.Config("skipLines" -> "24", "dataMarker" -> "\"data\":\\[")
      def adapter = new HapiJsonAdapter(model, config)
    }

    val ds = reader.getDataset
    TextWriter().write(ds)
  }
}