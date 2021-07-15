package latis.input

import java.net.URI

import org.scalatest.EitherValues._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.Inside.inside

import latis.data.DomainData
import latis.data.RangeData
import latis.data.Real
import latis.data.Sample
import latis.data.Text
import latis.dataset.AdaptedDataset
import latis.dsl.ModelParser
import latis.metadata.Metadata
import latis.model._
import latis.model.Scalar
import latis.ops.Selection
import latis.util.dap2.parser.ast._
import latis.util.FdmlUtils
import latis.util.StreamUtils
import latis.util.Identifier.IdentifierStringContext
import latis.time.Time

class HapiCsvAdapterSpec extends AnyFlatSpec {

  private lazy val dataset = {
    val model = ModelParser.unsafeParse("time: string -> irradiance: double")
    //  Function.from(
    //  Scalar(id"time", StringValueType),
    //  Scalar(id"irradiance", DoubleValueType)
    //).value

    val adapter = new HapiCsvAdapter(
      model,
      new HapiAdapter.Config(
        "id" -> "nrl2_tsi_P1Y"
      )
    )

    val baseUri = new URI("https://lasp.colorado.edu/lisird/hapi/")

    new AdaptedDataset(Metadata(id"nrl2_tsi_P1Y"), model, adapter, baseUri)
  }


  "A hapi csv request with time selections" should "return csv records" in {
    val ds = dataset
      .withOperation(Selection(id"time", Gt, "2010-01-01"))
      .withOperation(Selection(id"time", Lt, "2011-01-01"))

    inside(StreamUtils.unsafeHead(ds.samples)) {
      case Sample(DomainData(Text(time)), RangeData(Real(tsi))) =>
        time should be("2010-07-01T00:00:00.000Z")
        tsi should be(1360.785400390625)
    }
  }
  
  "A hapi request for vector data" should 
  "construct the paramaters list with a single parameter for the vector" in {
    // time -> (A, (V._1, V._2, V._3), B)
    val model = Function.from(
      Time.fromMetadata(Metadata("id" -> "time", "type" -> "string", "units" -> "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")).value,
      Tuple.fromElements(
        Scalar(id"A", DoubleValueType),
        Tuple.fromElements(id"V",
          Scalar(id"V._1", DoubleValueType),
          Scalar(id"V._2", DoubleValueType),
          Scalar(id"V._3", DoubleValueType)
        ).value,
        Scalar(id"B", DoubleValueType)
      ).value
    ).value
    
    HapiAdapter.buildParameterList(model) should be (List("A", "V", "B"))
  }

  "A HapiAdapter fdml file" should "validate" in {
    val fdmlFile = "datasets/sorce_tsi.fdml"
    FdmlUtils.validateFdml(fdmlFile) match {
      case Right(_) => //pass
      case Left(e) =>
        println(e.getMessage)
        fail()
    }
  }

  "A HapiAdapter" should "get the default time range from the info" in {
    val ds = dataset
      .withOperation(Selection(id"time", Lt, "1611"))
    ds.unsafeForce().data.length should be (1)
  }
}
