package latis.input

import latis.model.DataType

/**
 * Adapts a HAPI service as a source of data
 * using the binary output option.
 */
class HapiBinaryAdapter(model: DataType, config: HapiAdapter.Config)
  extends HapiAdapter(model, config) {

  /**
   * Defines the Adapter that will be used to parse the
   * results from the HAPI service call
   */
  def parsingAdapter: Adapter = new BinaryAdapter(model)

  /**
   * Defines the requested data format.
   * This must be consistent with the parsingAdapter.
   */
  def datasetFormat: String = "binary"
}

//=============================================================================

object HapiBinaryAdapter extends AdapterFactory {

  /**
   * Constructor used by the AdapterFactory.
   */
  def apply(model: DataType, config: AdapterConfig): HapiAdapter =
    new HapiBinaryAdapter(model, new HapiAdapter.Config(config.properties: _*))


}



