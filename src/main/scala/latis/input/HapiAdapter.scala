package latis.input

import java.net.URI

import latis.data.SampledFunction
import latis.model.DataType
import latis.ops.Operation
import latis.ops.Selection
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

  override def getData(
      baseUri: URI,
      ops: Seq[Operation] = Seq.empty
  ): SampledFunction = {
    val query = buildQuery(ops)
    val uri = new URI(baseUri.toString + "data?" + query) //TODO: make sure baseURI has separator
    parsingAdapter.getData(uri)
  }

  override def canHandleOperation(op: Operation): Boolean = op match {
    case Selection("time", _, _) => true
    //case p: Projection => true
    case _ => false
  }

  def buildQuery(ops: Seq[Operation]): String = {
    // Define the format of time strings used by the HAPI API.
    //val timeFormat = TimeFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

    // Placeholder for min time
    var startTime: Option[String] = None

    // Placeholder for max time
    var endTime: Option[String] = None

    // Build the query string from the operations
    val qs = ops map {
      case Selection("time", op, value) =>
        val time = value //TODO: timeFormat.format(value)
        op.head match {
          case '>' => "time.min=" + time
          case '<' => "time.max=" + time
          case '=' =>
            ??? //TODO: support single time selection: add epsilon, take first
          case _ =>
            val msg = s"Unsupported select operator: $op"
            throw new UnsupportedOperationException(msg)
        }
      //case Projection(vids @ _*) => vids.mkString("parameters=", ",", "")
      case _ =>
        ??? //TODO: bug, we should only be given Ops that pass canHandleOperation
    }
    //TODO: if we don't have both, use defaults from info header
    //TODO: add epsilon if times are the same? (CDAWeb seems to require it)
    if (qs.length != 2) { //TODO: redo test when we enable projection
      val msg = s"HAPI query requires min and max time selections"
      throw new UnsupportedOperationException(msg)
    }

    // Project only the variables defined in the model
    val projection = model.getScalars
      .drop(1) // time is implicit
      .map(_.id)
      .mkString("parameters=", ",", "")

    // Build the query
    Seq(
      s"id=${config.id}", // Add dataset ID
      qs.mkString("&"),
      projection,
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

}
