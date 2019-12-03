package latis.input

import latis.model.DataType

/**
 * Adapts a HAPI service as a source of data
 * using the csv output option.
 */
class HapiCsvAdapter(model: DataType, config: HapiAdapter.Config)
    extends HapiAdapter(model, config) {

  /**
   * Defines the Adapter that will be used to parse the
   * results from the HAPI service call
   */
  def parsingAdapter: Adapter = TextAdapter(model)

  /**
   * Defines the requested data format.
   * This must be consistent with the parsingAdapter.
   */
  def datasetFormat: String = "csv"

}

//=============================================================================

object HapiCsvAdapter extends AdapterFactory {

  /**
   * Constructor used by the AdapterFactory.
   */
  def apply(model: DataType, config: AdapterConfig): HapiAdapter =
    new HapiCsvAdapter(model, new HapiAdapter.Config(config.properties: _*))

}
