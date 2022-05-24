package latis.input

import java.net.URI

import munit.CatsEffectSuite

import latis.data._
import latis.dataset.AdaptedDataset
import latis.dsl.ModelParser
import latis.metadata.Metadata
import latis.model._
import latis.ops.Selection
import latis.time.Time
import latis.util.FdmlUtils
import latis.util.Identifier.IdentifierStringContext
import latis.util.dap2.parser.ast._

class HapiCsvAdapterSuite extends CatsEffectSuite {

  private lazy val dataset = {
    val model = ModelParser.unsafeParse("time: string -> irradiance: double")

    val adapter = new HapiCsvAdapter(
      model,
      new HapiAdapter.Config(
        "id" -> "nrl2_tsi_P1Y"
      )
    )

    val baseUri = new URI("https://lasp.colorado.edu/lisird/hapi/")

    new AdaptedDataset(Metadata(id"nrl2_tsi_P1Y"), model, adapter, baseUri)
  }


  test("read the first sample of a HAPI csv dataset with time selection") {
    val ds = dataset
      .withOperation(Selection(id"time", Gt, "2010-01-01"))
      .withOperation(Selection(id"time", Lt, "2011-01-01"))

    val samples = ds.samples.compile.toList
    samples.map {
      case Sample(DomainData(Text(time)), RangeData(Real(tsi))) :: _ =>
        assertEquals(time, "2010-07-01T00:00:00.000Z")
        assertEquals(tsi, 1360.785400390625)
      case _ => fail("Sample did not contain the expected data")
    }
  }

  test("construct the paramaters list with a single parameter for the vector") {
    // time -> (A, (V._1, V._2, V._3), B)
    val model = Function.from(
      Time.fromMetadata(
        Metadata(
          "id" -> "time",
          "type" -> "string",
          "units" -> "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
        )
      ).getOrElse(fail("Time not generated")),
      Tuple.fromElements(
        Scalar(id"A", DoubleValueType),
        Tuple.fromElements(id"V",
          Scalar(id"V._1", DoubleValueType),
          Scalar(id"V._2", DoubleValueType),
          Scalar(id"V._3", DoubleValueType)
        ).getOrElse(fail("Tuple not generated")),
        Scalar(id"B", DoubleValueType)
      ).getOrElse(fail("Tuple not generated"))
    ).getOrElse(fail("Model not generated"))

    assertEquals(HapiAdapter.buildParameterList(model), List("A", "V", "B"))
  }

  test("validate an fdml file") {
    val fdmlFile = "datasets/sorce_tsi.fdml"
    FdmlUtils.validateFdml(fdmlFile) match {
      case Right(_) => //pass
      case Left(e) =>
        fail("Failed with a LatisException: " + e.getMessage)
    }
  }

  test("get the default time range from the info") {
    val ds = dataset
      .withOperation(Selection(id"time", Lt, "1611"))

    assertEquals(ds.unsafeForce().data.length, 1)
  }
}
