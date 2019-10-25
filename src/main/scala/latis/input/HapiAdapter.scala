package latis.input

import latis.model._
import latis.util._
import java.net.URI
import latis.data.SampledFunction
import latis.data.HapiFunction

/**
 * Adapts a HAPI service as a source of data.
 */
class HapiAdapter(model: DataType, config: HapiAdapter.Config) extends Adapter {
  
  def apply(uri: URI): SampledFunction = 
    HapiFunction (
      model,
      TextAdapter(model, config),
      uri,
      config.query
    )
}

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
   * Configuration specific to a HapiAdapter.
   */
  class Config(properties: (String, String)*) extends TextAdapter.Config(properties: _*) {
    val query: String = getOrElse("query", "")
  }
  
}
