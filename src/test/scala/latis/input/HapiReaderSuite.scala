package latis.input

import java.net.URI

import cats.data.NonEmptyList
import munit.CatsEffectSuite

import latis.data._
import latis.model._
import latis.ops.Selection
import latis.time.Time
import latis.util.Identifier._
import latis.util.dap2.parser.ast._
import latis.util.hapi._

class HapiReaderSuite extends CatsEffectSuite {

  private lazy val reader = new HapiReader()

  test("read a dataset from a HAPI service") {
    val uri = new URI("https://lasp.colorado.edu/lisird/hapi/info?id=nrl2_tsi_P1Y")

    val ds = reader.read(uri)
      .getOrElse(fail("Did not get dataset."))
      .withOperation(Selection(id"time", Gt, "2010"))
      .withOperation(Selection(id"time", Lt,  "2011"))

    ds.id match {
      case Some(id) => assertEquals(id, id"nrl2_tsi_P1Y")
      case None => fail("No dataset id found")
    }

    ds.model match {
      case Function(d: Scalar, Tuple(i: Scalar, u: Scalar)) =>
        assertEquals(d.id, id"time")
        assertEquals(i.id, id"irradiance")
        assertEquals(u.id, id"uncertainty")
      case _ => fail("Model format match failed")
    }

    ds.samples.compile.toList.map {
      case Sample(DomainData(Text(time)), RangeData(Real(tsi), Real(unc))) :: _ =>
        assertEquals(time, "2010-07-01T00:00:00.000Z")
        assertEquals(tsi, 1360.785400390625)
        assertEquals(unc, 0.10427488386631012)
      case _ => fail("Sample format match failed")
    }
  }

  test("support scalar timeseries datasets") {
    val parameters: List[Parameter] = List(
      ScalarParameter("time", "isotime", Option("UTC"), Option(24), None),
      ScalarParameter("x", "string", Option("units"), None, None)
    )

    reader.toModel(parameters) match {
      case Some(Function(d: Time, r: Scalar)) =>
        assertEquals(d.id, id"time")
        assertEquals(r.id, id"x")
      case Some(model) => fail(s"Unexpected model: $model")
      case None => fail("Failed to construct model.")
    }
  }

  test("support vector timeseries datasets") {
    val parameters: List[Parameter] = List(
      ScalarParameter("time", "isotime", Option("UTC"), Option(24), None),
      VectorParameter("x", "string", Option("units"), None, None, NonEmptyList.of(3))
    )

    reader.toModel(parameters) match {
      case Some(Function(d: Time, Tuple(x0: Scalar, x1: Scalar, x2: Scalar))) =>
        assertEquals(d.id, id"time")
        assertEquals(x0.id, id"x._0")
        assertEquals(x1.id, id"x._1")
        assertEquals(x2.id, id"x._2")
      case Some(model) => fail(s"Unexpected model: $model")
      case None => fail("Failed to construct model.")
    }

  }

  test("support scalars and vectors in datasets") {
    val parameters: List[Parameter] = List(
      ScalarParameter("time", "isotime", Option("UTC"), Option(24), None),
      ScalarParameter("w", "string", Option("units"), None, None),
      VectorParameter("x", "string", Option("units"), None, None, NonEmptyList.of(2)),
      VectorParameter("y", "string", Option("units"), None, None, NonEmptyList.of(2)),
      ScalarParameter("z", "string", Option("units"), None, None)
    )

    reader.toModel(parameters) match {
      case Some(Function(d: Time, Tuple(w: Scalar, Tuple(x0: Scalar, x1: Scalar), Tuple(y0: Scalar, y1: Scalar), z: Scalar))) =>
        assertEquals(d.id, id"time")
        assertEquals(w.id, id"w")
        assertEquals(x0.id, id"x._0")
        assertEquals(x1.id, id"x._1")
        assertEquals(y0.id, id"y._0")
        assertEquals(y1.id, id"y._1")
        assertEquals(z.id, id"z")
      case Some(model) => fail(s"Unexpected model: $model")
      case None => fail("Failed to construct model.")
    }
  }

  test("support datasets with a vector as the first non-time parameter") {
    val parameters: List[Parameter] = List(
      ScalarParameter("time", "isotime", Option("UTC"), Option(24), None),
      VectorParameter("x", "string", Option("units"), None, None, NonEmptyList.of(2)),
      ScalarParameter("y", "string", Option("units"), None, None)
    )

    reader.toModel(parameters) match {
      case Some(Function(d: Time, Tuple(Tuple(x0: Scalar, x1: Scalar), y: Scalar))) =>
        assertEquals(d.id, id"time")
        assertEquals(x0.id, id"x._0")
        assertEquals(x1.id, id"x._1")
        assertEquals(y.id, id"y")
      case Some(model) => fail(s"Unexpected model: $model")
      case None => fail("Failed to construct model.")
    }
  }

  test("support array parameters in datasets") {
    val parameters: List[Parameter] = List(
      ScalarParameter("time", "isotime", Option("UTC"), Option(24), None),
      ArrayParameter("x", "string", Option("units"), None, None, NonEmptyList.of(1),
        NonEmptyList.of(Bin("w", "units"))
      )
    )

    reader.toModel(parameters) match {
      case Some(Function(d: Time, Function(w: Scalar, x: Scalar))) =>
        assertEquals(d.id, id"time")
        assertEquals(w.id, id"w")
        assertEquals(x.id, id"x")
      case Some(model) => fail(s"Unexpected model: $model")
      case None => fail("Failed to construct model.")
    }
  }

  test("support multiple array parameters in datasets") {
    val parameters: List[Parameter] = List(
      ScalarParameter("time", "isotime", Option("UTC"), Option(24), None),
      ArrayParameter("x", "string", Option("units"), None, None, NonEmptyList.of(1),
        NonEmptyList.of(Bin("w", "units"))
      ),
      ArrayParameter("y", "string", Option("units"), None, None, NonEmptyList.of(1),
        NonEmptyList.of(Bin("w", "units"))
      ),
      ArrayParameter("z", "string", Option("units"), None, None, NonEmptyList.of(1),
        NonEmptyList.of(Bin("w", "units"))
      )
    )

    reader.toModel(parameters) match {
      case Some(Function(d: Time, Function(w: Scalar, Tuple(x: Scalar, y: Scalar, z: Scalar)))) =>
        assertEquals(d.id, id"time")
        assertEquals(w.id, id"w")
        assertEquals(x.id, id"x")
        assertEquals(y.id, id"y")
        assertEquals(z.id, id"z")
      case Some(model) => fail(s"Unexpected model: $model")
      case None => fail("Failed to construct model.")
    }
  }

  test("gracefully reject mixing array parameters with different bins") {
    val parameters: List[Parameter] = List(
      ScalarParameter("time", "isotime", Option("UTC"), Option(24), None),
      ArrayParameter("x", "string", Option("units"), None, None, NonEmptyList.of(1),
        NonEmptyList.of(Bin("w", "units"))
      ),
      ArrayParameter("y", "string", Option("units"), None, None, NonEmptyList.of(1),
        NonEmptyList.of(Bin("z", "units"))
      )
    )

    assertEquals(reader.toModel(parameters), None)
  }

  test("gracefully reject mixing array and other parameters") {
    val parameters: List[Parameter] = List(
      ScalarParameter("time", "isotime", Option("UTC"), Option(24), None),
      ScalarParameter("x", "string", Option("units"), None, None),
      ArrayParameter("y", "string", Option("units"), None, None, NonEmptyList.of(1),
        NonEmptyList.of(Bin("w", "units"))
      )
    )

    assertEquals(reader.toModel(parameters), None)
  }

  test("gracefully reject responses with only the time parameter") {
    val parameters: List[Parameter] = List(
      ScalarParameter("time", "isotime", Option("UTC"), Option(24), None)
    )

    assertEquals(reader.toModel(parameters), None)
  }
}
