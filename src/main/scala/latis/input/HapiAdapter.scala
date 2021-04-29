package latis.input

import java.net.URI

import io.circe._
import io.circe.parser._

import latis.data.Data
import latis.model.DataType
import latis.ops.Operation
import latis.ops.Selection
import latis.time.TimeFormat
import latis.util.ConfigLike
import latis.util.dap2.parser.ast._
import latis.util.Identifier
import latis.util.LatisException
import latis.util.NetUtils
import latis.util.hapi.Info

/**
 * Adapts a HAPI service as a source of data.
 */
abstract class HapiAdapter(model: DataType, config: HapiAdapter.Config) extends Adapter {

  /**
   * Defines the Adapter that will be used to parse the
   * results from the HAPI service call
   */
  def parsingAdapter: Adapter

  /**
   * Defines the requested data format.
   * This must be consistent with the parsingAdapter.
   */
  def datasetFormat: String

  /**
   * Saves the base HAPI URI as a string to be used to build requests.
   * Note, this is only available after the data request has been made.
   */
  private var baseUriString: String = _

  /** Defines the format of time strings used by the HAPI API. */
  val timeFormat: TimeFormat = TimeFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") //TODO: consider relaxing this

  /**
   * Applies the given Operations by building and appending
   * a query to the given base URI. The parsingAdapter will
   * be used to request and parse the data into a SampledFunction.
   */
  override def getData(
    baseUri: URI,
    ops: Seq[Operation] = Seq.empty
  ): Data = {
    baseUriString = baseUri.toString match {
      // Makes sure the baseUri ends with a separator
      case s if s.endsWith("/") => s
      case s                    => s + "/"
    }
    val query = buildQuery(ops)
    val uri = new URI(baseUriString + "data?" + query)
    parsingAdapter.getData(uri)
  }

  /**
   * Tells the caller whether this Adapter can handle the given
   * Operation.
   */
  override def canHandleOperation(op: Operation): Boolean = op match {
    case Selection(Identifier("time"), _, _) => true
    //case p: Projection => true
    case _ => false
  }

  /**
   * Constructs a HAPI query from the given sequence of
   * Operations.
   */
  def buildQuery(ops: Seq[Operation]): String = {

    // Placeholder for min time
    var (startTime, stopTime) = defaultTimeCoverage()

    // Updates query info based on each Operation
    ops.foreach {
      case Selection(Identifier("time"), op, value) =>
        val time = TimeFormat.parseIso(value).getOrElse {
          val msg = s"Failed to parse time: $value"
          throw LatisException(msg)
        } //ms since 1970
        op match {
          case Gt | GtEq =>
            if (time > startTime) startTime = time
          case Lt | LtEq =>
            if (time < stopTime) stopTime = time
          case Eq =>
            startTime = time
            stopTime  = time
          case _ =>
            val msg = s"Unsupported select operator: ${prettyOp(op)}"
            throw LatisException(msg)
        }
      //case Projection(vids @ _*) => vids.mkString("parameters=", ",", "")
      case o =>
        val msg = s"HapiAdapter is not able to apply the operation: $o"
        throw LatisException(msg)
      // We should only be given Ops that pass canHandleOperation
    }

    // Makes sure time selection is valid
    (stopTime - startTime) match {
      case n if (n < 0) =>
        val msg = "Start time must be less than end time."
        throw LatisException(msg)
      case n if (n < 1000) =>
        // Add epsilon if times are within one second
        // CDAWeb seems to require it
        stopTime = startTime + 1000
      case _ => //good to go
    }

    // Projects only the variables defined in the model
    val params = HapiAdapter
      .buildParameterList(model)
      .mkString("parameters=", ",", "")

    // Builds the query
    Seq(
      s"id=${config.id}", // Add dataset ID
      s"time.min=${timeFormat.format(startTime)}",
      s"time.max=${timeFormat.format(stopTime)}",
      params,
      s"format=$datasetFormat" // Add dataset format
    ).mkString("&")
  }

  /** Defines the HAPI info request URI */
  def infoUri: URI =
    new URI(baseUriString + "info?" + s"id=${config.id}")

  /** Reads the info response into an Info object. */
  def readInfo(): Info = {
    val either = for {
      s    <- NetUtils.readUriIntoString(infoUri)
      json <- parse(s)
      info <- Decoder[Info].decodeJson(json)
    } yield info
    either.getOrElse {
      val msg = s"Failed to get info response from $infoUri"
      throw LatisException(msg)
    }
  }

  /** Gets the time coverage from the HAPI info. */
  //TODO: use the HAPI sampleStartDate and sampleStopDate if available
  //  See: https://github.com/latis-data/latis3-hapi/issues/9
  def defaultTimeCoverage(): (Long, Long) = {
    val info = readInfo()
    val either = for {
      start <- TimeFormat.parseIso(info.startDate)
      stop  <- TimeFormat.parseIso(info.stopDate)
    } yield (start, stop)
    either.getOrElse {
      val msg = "Failed to get time coverage from HAPI info"
      throw LatisException(msg)
    }

  }
}

//=============================================================================

object HapiAdapter {

  /**
   * Configuration specific to a HapiAdapter.
   * It requires that a dataset identifier be defined as "id".
   */
  class Config(val properties: (String, String)*) extends ConfigLike {
    val id: String = get("id") getOrElse {
      val msg = "The HAPI dataset does not have an id."
      throw new RuntimeException(msg)
    }
  }

  /**
   * Given a FDM model, create a list of variable names
   * for the HAPI "parameters" query parameter.
   * Note that vector components with IDs: "foo._1", "foo._2"
   * will be reduced to a single "foo" parameter.
   */
  def buildParameterList(model: DataType): List[String] =
    model.getScalars
      .drop(1) //drop implicit time variable
      .map(_.id.fold("")(_.asString)) //get the IDs as Strings
      .map(_.split("\\._\\d").head)
      .distinct //reduce vector elements
}
