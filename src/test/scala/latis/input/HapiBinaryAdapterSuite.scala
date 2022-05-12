package latis.input

import java.net.URI

import munit.CatsEffectSuite

import latis.data._
import latis.metadata.Metadata
import latis.model._
import latis.time.Time
import latis.util.FileUtils
import latis.util.Identifier.IdentifierStringContext

class HapiBinaryAdapterSuite extends CatsEffectSuite {

  private lazy val uri: URI = FileUtils.resolvePath("data/hapi_binary_data").get.toUri

  private lazy val reader = new AdaptedDatasetReader {

    def model: DataType = Function.from(
      Time.fromMetadata(Metadata("id" -> "Time", "type" -> "string", "size" -> "24", "units"->"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")).getOrElse(fail("Time not generated")),
      Tuple.fromElements(
        Scalar(id"Magnitude", DoubleValueType),
        Scalar(id"dBrms", DoubleValueType)
      ).getOrElse(fail("Tuple not generated"))
    ).getOrElse(fail("Model not generated"))

    def metadata: Metadata = Metadata(id"hapi_binary")

    def adapter: Adapter = HapiBinaryAdapter(model, AdapterConfig(("class","HAPI"), ("id","hapi_binary_data"))).parsingAdapter
  }

  private lazy val ds = reader.read(uri)

  test("read the correct values of a HAPI binary dataset") {
    val samples = ds.samples.compile.toList
    samples.map { s =>
      s.head match {
        case Sample(DomainData(Text(time)), RangeData(Real(mag), Real(dbrms))) =>
          assertEquals(time, "2019-01-01T00:00:00.000Z")
          assertEquals(mag, -1.0E31)
          assertEquals(dbrms, -1.0E31)
        case _ => fail("Sample did not contain the expected data")
      }
    }
  }

}
