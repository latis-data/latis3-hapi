package latis.input

import java.net.URI

import latis.data.SampledFunction
import latis.model.DataType
import latis.ops.Operation
import latis.ops.Selection
import latis.time.TimeFormat
import latis.util.ConfigLike

/**
 * Adapts a HAPI service as a source of data.
 */
abstract class HapiAdapter(model: DataType, config: HapiAdapter.Config)
    extends Adapter {

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
   * Applies the given Operations by building and appending
   * a query to the given base URI. The parsingAdapter will
   * be used to request and parse the data into a SampledFunction.
   */
  override def getData(
    baseUri: URI,
    ops: Seq[Operation] = Seq.empty
  ): SampledFunction = {
    val query = buildQuery(ops)
    val uri = new URI(baseUri.toString + "data?" + query) //TODO: make sure baseURI has separator
    parsingAdapter.getData(uri)
  }

  /**
   * Tells the caller whether this Adapter can handle the given
   * Operation.
   */
  override def canHandleOperation(op: Operation): Boolean = op match {
    case Selection("time", _, _) => true
    //case p: Projection => true
    case _ => false
  }

  /**
   * Constructs a HAPI query from the given sequence of
   * Operations.
   */
  def buildQuery(ops: Seq[Operation]): String = {
    // Define the format of time strings used by the HAPI API.
    val timeFormat = TimeFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

    // Placeholder for min time
    var startTime: Long = TimeFormat.parseIso("1900-01-01").getOrElse(???)
    //TODO: default from info

    // Placeholder for max time
    var endTime: Long = TimeFormat.parseIso("2100-01-01").getOrElse(???)
    //TODO: default from info

    // Update query info based on each Operation
    ops foreach {
      case Selection("time", op, value) =>
        val time = TimeFormat.parseIso(value).getOrElse(???) //ms since 1970
        op.head match {
          case '>' =>
            if (time > startTime) startTime = time
          case '<' =>
            if (time < endTime) endTime = time
          case '=' =>
            startTime = time
            endTime = time
          case _ =>
            val msg = s"Unsupported select operator: $op"
            throw new UnsupportedOperationException(msg)
        }
      //case Projection(vids @ _*) => vids.mkString("parameters=", ",", "")
      case o =>
        val msg = s"HapiAdapter is not able to apply the operation: $o"
        throw new UnsupportedOperationException(msg)
      // We should only be given Ops that pass canHandleOperation
    }

    // Make sure time selection is valid
    (endTime - startTime) match {
      case n if n < 0 =>
        val msg = "Start time must be less than end time."
        throw new UnsupportedOperationException(msg)
      case n if n < 1000 =>
        // Add epsilon if times are within one second
        // CDAWeb seems to require it
        endTime = startTime + 1000
      case _ => //good to go
    }

    // Project only the variables defined in the model
    val params = HapiAdapter
      .buildParameterList(model)
      .mkString("parameters=", ",", "")

    // Build the query
    Seq(
      s"id=${config.id}", // Add dataset ID
      s"time.min=${timeFormat.format(startTime)}",
      s"time.max=${timeFormat.format(endTime)}",
      params,
      s"format=$datasetFormat" // Add dataset format
    ).mkString("&")
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
      .map(_.id) //get the IDs
      .map(_.split("\\._\\d").head)
      .distinct //reduce vector elements
}
