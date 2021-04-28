package latis.input

import java.net.URI

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import latis.data._
import latis.model._
import latis.ops.Selection
import latis.time.Time
import latis.util.dap2.parser.ast._
import latis.util.Identifier.IdentifierStringContext
import latis.util.StreamUtils
import latis.util.hapi._

class HapiReaderSpec extends FlatSpec with Matchers {

  val reader = new HapiReader()

  "A HAPI reader" should "read a dataset from a HAPI service" in {
    val uri = new URI("https://lasp.colorado.edu/lisird/hapi/info?id=nrl2_tsi_P1Y")

    val ds = reader.read(uri)
      .getOrElse(fail("Did not get dataset."))
      .withOperation(Selection(id"time", Gt, "2010"))
      .withOperation(Selection(id"time", Lt,  "2011"))

    ds.id.get should be (id"nrl2_tsi_P1Y")

    ds.model match {
      case Function(d: Scalar, Tuple(i: Scalar, u: Scalar)) =>
        d.id.get should be (id"time")
        i.id.get should be (id"irradiance")
        u.id.get should be (id"uncertainty")
    }

    StreamUtils.unsafeHead(ds.samples) match {
      case Sample(DomainData(Text(time)), RangeData(Real(tsi), Real(unc))) =>
        time should be ("2010-07-01T00:00:00.000Z")
        tsi should be (1360.785400390625)
        unc should be (0.10427488386631012)
    }
  }

  it should "support scalar timeseries datasets" in {
    val parameters: List[Parameter] = List(
      ScalarParameter("time", "isotime", "UTC", Option(24), None),
      ScalarParameter("x", "string", "units", None, None)
    )

    val model: DataType = reader.toModel(parameters).getOrElse(
      fail("Failed to construct model.")
    )

    model match {
      case Function(d: Time, r: Scalar) =>
        d.id.get should be (id"time")
        r.id.get should be (id"x")
      case _ => fail(s"Unexpected model: $model")
    }
  }

  it should "support vector timeseries datasets" in {
    val parameters: List[Parameter] = List(
      ScalarParameter("time", "isotime", "UTC", Option(24), None),
      VectorParameter("x", "string", "units", None, None, 3)
    )

    val model: DataType = reader.toModel(parameters).getOrElse(
      fail("Failed to construct model.")
    )

    model match {
      case Function(d: Time, Tuple(x0, x1, x2)) =>
        d.id.get should be (id"time")
        x0.id.get should be (id"x._0")
        x1.id.get should be (id"x._1")
        x2.id.get should be (id"x._2")
      case _ => fail(s"Unexpected model: $model")
    }
  }

  it should "support scalars and vectors in datasets" in {
    val parameters: List[Parameter] = List(
      ScalarParameter("time", "isotime", "UTC", Option(24), None),
      ScalarParameter("w", "string", "units", None, None),
      VectorParameter("x", "string", "units", None, None, 2),
      VectorParameter("y", "string", "units", None, None, 2),
      ScalarParameter("z", "string", "units", None, None)
    )

    val model: DataType = reader.toModel(parameters).getOrElse(
      fail("Failed to construct model.")
    )

    model match {
      case Function(d: Time, Tuple(w, Tuple(x0, x1), Tuple(y0, y1), z)) =>
        d.id.get should be (id"time")
        w.id.get should be (id"w")
        x0.id.get should be (id"x._0")
        x1.id.get should be (id"x._1")
        y0.id.get should be (id"y._0")
        y1.id.get should be (id"y._1")
        z.id.get should be (id"z")
      case _ => fail(s"Unexpected model: $model")
    }
  }

  it should "support datasets with a vector as the first non-time parameter" in {
    val parameters: List[Parameter] = List(
      ScalarParameter("time", "isotime", "UTC", Option(24), None),
      VectorParameter("x", "string", "units", None, None, 2),
      ScalarParameter("y", "string", "units", None, None)
    )

    val model: DataType = reader.toModel(parameters).getOrElse(
      fail("Failed to construct model.")
    )

    model match {
      case Function(d: Time, Tuple(Tuple(x0, x1), y)) =>
        d.id.get should be (id"time")
        x0.id.get should be (id"x._0")
        x1.id.get should be (id"x._1")
        y.id.get should be (id"y")
      case _ => fail(s"Unexpected model: $model")
    }
  }

  it should "support array parameters in datasets" in {
    val parameters: List[Parameter] = List(
      ScalarParameter("time", "isotime", "UTC", Option(24), None),
      ArrayParameter("x", "string", "units", None, None, 1,
        Bin("w", "units")
      )
    )

    val model: DataType = reader.toModel(parameters).getOrElse(
      fail("Failed to construct model.")
    )

    model match {
      case Function(d: Time, Function(w: Scalar, x: Scalar)) =>
        d.id.get should be (id"time")
        w.id.get should be (id"w")
        x.id.get should be (id"x")
      case _ => fail(s"Unexpected model: $model")
    }
  }

  it should "support multiple array parameters in datasets" in {
    val parameters: List[Parameter] = List(
      ScalarParameter("time", "isotime", "UTC", Option(24), None),
      ArrayParameter("x", "string", "units", None, None, 1,
        Bin("w", "units")
      ),
      ArrayParameter("y", "string", "units", None, None, 1,
        Bin("w", "units")
      ),
      ArrayParameter("z", "string", "units", None, None, 1,
        Bin("w", "units")
      )
    )

    val model: DataType = reader.toModel(parameters).getOrElse(
      fail("Failed to construct model.")
    )

    model match {
      case Function(d: Time, Function(w: Scalar, Tuple(x: Scalar, y: Scalar, z: Scalar))) =>
        d.id.get should be (id"time")
        w.id.get should be (id"w")
        x.id.get should be (id"x")
        y.id.get should be (id"y")
        z.id.get should be (id"z")
      case _ => fail(s"Unexpected model: $model")
    }
  }

  it should "gracefully reject mixing array parameters with different bins" in {
    val parameters: List[Parameter] = List(
      ScalarParameter("time", "isotime", "UTC", Option(24), None),
      ArrayParameter("x", "string", "units", None, None, 1,
        Bin("w", "units")
      ),
      ArrayParameter("y", "string", "units", None, None, 1,
        Bin("z", "units")
      )
    )

    reader.toModel(parameters) should be (None)
  }

  it should "gracefully reject mixing array and other parameters" in {
    val parameters: List[Parameter] = List(
      ScalarParameter("time", "isotime", "UTC", Option(24), None),
      ScalarParameter("x", "string", "units", None, None),
      ArrayParameter("y", "string", "units", None, None, 1,
        Bin("w", "units")
      )
    )

    reader.toModel(parameters) should be (None)
  }

  it should "gracefully reject responses with only the time parameter" in {
    val parameters: List[Parameter] = List(
      ScalarParameter("time", "isotime", "UTC", Option(24), None)
    )

    reader.toModel(parameters) should be (None)
  }
}
