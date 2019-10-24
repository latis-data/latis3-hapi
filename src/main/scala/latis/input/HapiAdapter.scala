package latis.input

import latis.model._
import latis.util._
import java.net.URI
import latis.data.SampledFunction
import latis.data.HapiFunction

class HapiAdapter(model: DataType, config: HapiAdapter.Config) extends Adapter {
  
  def apply(uri: URI): SampledFunction = 
    HapiFunction (
      model,
      TextAdapter(model, config),
      uri,
      config.query
    )
}

/*
 * need a SampledFunction that can lazily apply ops via the hapi api
 * This can have a config so we can define options
 * 
 * How can we stitch in the TextAdapter capabilities
 * separate parsing from building request
 * 
 * special class for service wrapping adapters?
 * it is unique in that the URL changes based on the operations
 * seems that we need a SF that can build the URL 
 * embed parsing adapter?
 *   
 * 
 * 
 */

//=============================================================================

object HapiAdapter extends AdapterFactory {
  
  def apply(model: DataType, config: Config = new Config()): HapiAdapter = 
    new HapiAdapter(model, config)
  
  /**
   * Constructor used by the AdapterFactory.
   */
  def apply(model: DataType, config: AdapterConfig): HapiAdapter = 
    new HapiAdapter(model, new Config(config.properties: _*))
  
  /**
   * Configuration specific to a TextAdapter.
   */
  class Config(properties: (String, String)*) extends TextAdapter.Config(properties: _*) {
    val query: String = getOrElse("query", "")
  }
  
}
