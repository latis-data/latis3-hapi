package latis.input

import latis.model.DataType

/**
 * Adapter for parsing HAPI JSON data.
 */
class JsonArrayAdapter(model: DataType)
    extends TextAdapter(
      model,
      new TextAdapter.Config("dataMarker" -> "\"data\":\\[")
    ) {

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
    str.trim.split(",").toVector

  /**
   * Remove all square brackets and/or quotes from the edges of a string.
   */
  def removeBracketsAndQuotes(str: String): String =
    str.replaceAll("^(\\[\")|^[\\[\"]|[\"\\]]$|(\"\\])$", "")
}
