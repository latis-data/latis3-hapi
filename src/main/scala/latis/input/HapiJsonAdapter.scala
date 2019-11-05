package latis.input

import latis.model.DataType

/**
 * Adapter for HAPI JSON datasets.
 */
class HapiJsonAdapter(model: DataType, config: TextAdapter.Config = new TextAdapter.Config("dataMarker" -> "\"data\":\\["))
  extends TextAdapter(model, config) {

  /**
   * Extract the data values from the given record.
   */
  override def extractValues(record: String): Vector[String] =
    splitAtDelim(record).map(removeBracketsAndQuotes(_))

  /**
   * Split the given string based on the configured delimiter.
   * The delimiter can be a regular expression.
   * A trailing delimiter will not yield an empty string.
   */
  override def splitAtDelim(str: String): Vector[String] =
    str.trim.split(config.delimiter).toVector

  /**
   * Remove all square brackets and/or quotes from the edges of a string.
   */
  def removeBracketsAndQuotes(str: String): String =
    str.replaceAll("^(\\[\")|^[\\[\"]|[\"\\]]$|(\"\\])$", "")
}

//=============================================================================

object HapiJsonAdapter extends AdapterFactory {

  def apply(model: DataType, config: TextAdapter.Config = new TextAdapter.Config("dataMarker" -> "\"data\":\\[")): HapiJsonAdapter =
    new HapiJsonAdapter(model, config)

  /**
   * Constructor used by the AdapterFactory.
   */
  def apply(model: DataType, config: AdapterConfig): HapiJsonAdapter =
    new HapiJsonAdapter(model, new TextAdapter.Config(config.properties: _*))

}
