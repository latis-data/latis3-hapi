package latis.input

import java.net.URI

import scala.concurrent.ExecutionContext

import cats.effect.IO
import cats.effect.Resource
import cats.implicits._
import io.circe.Decoder
import io.circe.Json
import org.http4s.Uri
import org.http4s.client.Client
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.circe._

import latis.dataset.AdaptedDataset
import latis.dataset.Dataset
import latis.metadata.Metadata
import latis.model._
import latis.time.Time
import latis.util.hapi._
import latis.util.StreamUtils.contextShift

/**
 * A reader for datasets accessible though a HAPI service.
 *
 * This reader makes some assumptions about the dataset being read:
 *
 * - At least one range variable has been projected.
 * - Every range variable is a function of every domain variable.
 * - The dataset is a timeseries or has a single nested function.
 *
 * These assumptions translate to the following requirements on the
 * HAPI parameters:
 *
 * - If any parameter has bins, all parameters are assumed to have the
 *   same set of bins.
 * - Every parameter has zero bins, or every parameter has the same
 *   bin.
 * - The size of every parameter is undefined (a scalar) or is one
 *   dimensional (a tuple or array).
 */
class HapiReader extends DatasetReader {

  /**
   * Makes a LaTiS Dataset from a HAPI info request.
   *
   * @param uri URI for HAPI info request for a dataset
   */
  override def read(uri: URI): Option[Dataset] = for {
    id      <- getId(uri)
    json    <- makeInfoRequest(uri) match {
      case Right(json) => Option(json)
      case Left(err)   => throw err
    }
    _       <- isHapiResponse(json).guard[Option]
    baseUri <- getBaseUri(uri)
    info    <- parseInfo(json) match {
      case Right(info) => Option(info)
      case Left(err)   => throw err
    }
    metadata = Metadata(id)
    model   <- toModel(info.parameters)
    adapter  = new HapiCsvAdapter(
      model,
      new HapiAdapter.Config(
        "class" -> "latis.input.HapiCsvAdapter",
        "id"    -> id
      )
    )
    dataset  = new AdaptedDataset(metadata, model, adapter, baseUri)
  } yield dataset

  private def httpClient: Resource[IO, Client[IO]] =
    BlazeClientBuilder[IO](ExecutionContext.global).resource

  private def makeInfoRequest(uri: URI): Either[Throwable, Json] = for {
    infoUri <- Uri.fromString(uri.toString())
    json    <- httpClient.use(_.expect[Json](infoUri)).attempt.unsafeRunSync()
  } yield json

  private def parseInfo(json: Json): Either[Throwable, Info] =
    Decoder[Info].decodeJson(json)

  private def isHapiResponse(json: Json): Boolean =
    json.hcursor.get[String]("HAPI").isRight

  /** Parses dataset ID from a HAPI request. */
  private def getId(infoUri: URI): Option[String] = for {
    query <- Option(infoUri.getQuery())
    regex  = """id=([^&]+)""".r
    mtch  <- regex.findFirstMatchIn(query)
    id    <- if (mtch.groupCount > 0) Option(mtch.group(1)) else None
  } yield id

  /** Gets the base URI of a HAPI service from the info URI. */
  private def getBaseUri(infoUri: URI): Option[URI] = for {
    scheme <- Option(infoUri.getScheme())
    host   <- Option(infoUri.getHost())
    path   <- Option(infoUri.getPath())
    newPath = path.stripSuffix("info")
    uri     = new URI(scheme, host, newPath, null)
  } yield uri

  /** Constructs the model from a list of HAPI parameters. */
  private[input] def toModel(ps: List[Parameter]): Option[DataType] = ps match {
    // Time will always be first, and we require that it be followed
    // by at least one other parameter.
    case (time: ScalarParameter) :: first :: rest =>
      // This fold builds the range of the dataset by turning each
      // HAPI parameter into a DataType and adding it to the model if
      // it doesn't violate our stated assumptions.
      val range: Option[DataType] = {
        val dt = toDataType(first)
        rest.foldM(dt)(addParameterToModel)
      }

      range.map(Function(toTime(time), _))
    case _ => None
  }

  /** Adds a HAPI parameter to the LaTiS model. */
  private val addParameterToModel: (DataType, Parameter) => Option[DataType] = {
    // If there is a function in the range, the next parameter
    // must share the same domain. (We check this by comparing the
    // name of the next parameter's bin to the name of the domain
    // of the function.)
    case (Function(d: Scalar, r), p: ArrayParameter) if p.bin.name == d.id =>
      val np = ScalarParameter(p.name, p.typeName, p.units, p.length, p.fill)
      val newRange = addParameterToModel(r, np)
      newRange.map(Function(d, _))
    // Array parameters can only be placed in functions.
    case (_, _: ArrayParameter) => None
    // The next parameter for these cases will either be a scalar
    // or a vector, and both are ok to add if we don't already
    // have a function in the range.
    case (s: Scalar, p)          => Option(Tuple(s, toDataType(p)))
    case (t @ Tuple(xs @ _*), p) => if (t.id.isEmpty) {
      // If the tuple's ID is an empty string, it is the anonymous
      // tuple grouping the range variables.
      Option(Tuple((xs :+ toDataType(p)): _*))
    } else {
      // If the existing tuple has an ID, it came from a vector.
      Option(Tuple(t, toDataType(p)))
    }
    case _ => None
  }

  /** Constructs a LaTiS DataType from a HAPI parameter. */
  private def toDataType(p: Parameter): DataType = p match {
    case p: ScalarParameter => toScalar(p)
    case p: VectorParameter => toTuple(p)
    case p: ArrayParameter  => toFunction(p)
  }

  /** Constructs a LaTiS Function from a HAPI parameter. */
  private def toFunction(p: ArrayParameter): Function = p match {
    case ArrayParameter(name, tyName, units, length, fill, _, Bin(bName, bUnits)) =>
      // The domain of the function is the bin as a Scalar.
      val d: DataType = toScalar(
        ScalarParameter(bName, "double", bUnits, None, None)
      )

      // The range of the function is the array parameter as a Scalar.
      val r: DataType = toScalar(
        ScalarParameter(name, tyName, units, length, fill)
      )

      Function(d, r)
  }

  /** Constructs a LaTiS Scalar from a HAPI parameter. */
  private def toScalar(p: ScalarParameter): Scalar = p match {
    case ScalarParameter(name, tyName, units, length, fill) =>
      val md = makeMetadata(name, tyName, units, length, fill)
      Scalar(md)
  }

  /** Constructs a LaTiS Tuple from a HAPI parameter. */
  private def toTuple(p: VectorParameter): Tuple = p match {
    case VectorParameter(name, tyName, units, length, fill, size) =>
      val ds: List[DataType] = List.tabulate(size) { n =>
        val md = makeMetadata(s"${name}._$n", tyName, units, length, fill)
        Scalar(md)
      }
      Tuple(Metadata("id" -> name), ds:_*)
  }

  /** Constructs a LaTiS Time value from a HAPI parameter. */
  private def toTime(p: ScalarParameter): Time = p match {
    case ScalarParameter(_, _, _, l @ Some(length), _) =>
      val md = makeMetadata("time", "string", getTimeFormat(length), l, None)
      Time(md)
    case _ => throw new RuntimeException("Time parameter requires length.")
  }

  /** Determines time format from reported time string length. */
  private def getTimeFormat(length: Int): String = length match {
    case 24 => "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
    case 22 => "yyyy-ddd'T'HH:mm:ss.SSS'Z'"
    case 20 => "yyyy-MM-dd'T'HH:mm:ss'Z'"
    case 18 => "yyyy-ddd'T'HH:mm:ss'Z'"
    case 17 => "yyyy-MM-dd'T'HH:mm'Z'"
    case 15 => "yyyy-ddd'T'HH:mm'Z'"
    case 14 => "yyyy-MM-dd'T'HH'Z'"
    case 12 => "yyyy-ddd'T'HH'Z'"
    case 11 => "yyyy-MM-dd'Z'"
    case 9  => "yyyy-ddd'Z'"
    case 8  => "yyyy-MM'Z'"
    case 5  => "yyyy'Z'"
    case _ => throw new RuntimeException("Could not determine time format.")
  }

  /** Makes LaTiS Metadata from HAPI parameter metadata. */
  private def makeMetadata(
    id: String,
    tyName: String,
    units: String,
    length: Option[Int],
    fill: Option[String]
  ): Metadata = {
    val tn = tyName match {
      case "isotime" => "string"
      case "integer" => "int"
      case t         => t
    }

    val b = Metadata("id" -> id, "type" -> tn, "units" -> units)
    val l = length.fold(Metadata())(l => Metadata("length" -> l.toString()))
    val f = fill.fold(Metadata())(f => Metadata("fill" -> f))

    b ++ l ++ f
  }
}
