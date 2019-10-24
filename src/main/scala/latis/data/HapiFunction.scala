package latis.data

import java.net.URI
import latis.model.DataType
import latis.input.Adapter
import latis.ops.FunctionalAlgebra.ImplicitOps._

case class HapiFunction(
  model: DataType, 
  adapter: Adapter,
  baseUri: URI, 
  query: String = ""
) extends SampledFunction {
  /*
   * Assumes:
   * time values formatted as HAPI wants them
   * time selections for min, max
   * no results if the same
   * no exclusivity consideration
   * no other selections
   */
  
  /**
   * Define the format of time strings used by the HAPI API.
   */
  private val timeFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
  
  /**
   * Append the new query to the existing query.
   */
  private def appendToQuery(newQuery: String): String = 
    query + "&" + newQuery
  
  /**
   * Appends the given selection to the query if it is for time.
   * Otherwise it delegates to the default SampledFunction implementation.
   */
  def select(vname: String, operator: String, value: String): SampledFunction = {
    if (vname == "time") {
      val time = value //TODO: format time 
      val newQuery = operator.head match {
        case '>' => "time.min=" + time
        case '<' => "time.max=" + time
        case '=' => ??? //TODO: support single time selection: add epsilon, take first
      }
      //TODO: add epsilon if times are the same
      this.copy(query = appendToQuery(newQuery))
    }
      
    else this.asInstanceOf[SampledFunction].select(vname, operator, value)
  }
  
  /**
   * Construct the final HAPI request URL and invoke the given Adapter
   * to return an fs2.Stream of Samples.
   */
  def streamSamples = {
    val uri = buildUri()
    adapter(uri).streamSamples
  }
  
  /**
   * Constructs the HAPI service HTTP request.
   */
  def buildUri(): URI = new URI(baseUri.toString + "data?" + query)
    
  
  def isEmpty: Boolean = ???
}