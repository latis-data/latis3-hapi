package latis.input

import latis.model.DataType

/**
 * Adapts a HAPI service as a source of data
 * using the json output option.
 */
class HapiJsonAdapter(model: DataType, config: HapiAdapter.Config)
  extends HapiAdapter(model, config) {
 
  /**
   * Defines the Adapter that will be used to parse the
   * results from the HAPI service call
   */
  def parsingAdapter: Adapter = new JsonArrayAdapter(model)
  
  /**
   * Defines the requested data format.
   * This must be consistent with the parsingAdapter.
   */
  def datasetFormat: String = "json"
}

//=============================================================================

object HapiJsonAdapter extends AdapterFactory {

  /**
   * Constructor used by the AdapterFactory.
   */
  def apply(model: DataType, config: AdapterConfig): HapiAdapter = 
    new HapiJsonAdapter(model, new HapiAdapter.Config(config.properties: _*))

}
