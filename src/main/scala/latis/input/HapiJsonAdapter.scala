package latis.input

import java.net.URI

import cats.effect.IO
import fs2.{Stream, text}
import latis.data.Sample
import latis.model.{DataType, Function}
import latis.util.ConfigLike

/**
 * Adapter for HAPI JSON datasets.
 */
class HapiJsonAdapter(model: DataType, config: TextAdapter.Config = new TextAdapter.Config())
  extends TextAdapter(model) {

  /**
   * Extract the data values from the given record.
   */
  override def extractValues(record: String): Vector[String] = {
    println(record)
    splitAtDelim(record)
  }

  /**
   * Split the given string based on the configured delimiter.
   * The delimiter can be a regular expression.
   * A trailing delimiter will yield an empty string.
   */
  override def splitAtDelim(str: String): Vector[String] =
    str.trim.split(config.delimiter, -1).toVector
  //Note, use "-1" so trailing ","s will yield empty strings.
  //TODO: StringUtil?
}

//=============================================================================

object HapiJsonAdapter extends AdapterFactory {

  def apply(model: DataType, config: TextAdapter.Config = new TextAdapter.Config()): HapiJsonAdapter =
    new HapiJsonAdapter(model, config)

  /**
   * Constructor used by the AdapterFactory.
   */
  def apply(model: DataType, config: AdapterConfig): HapiJsonAdapter =
    new HapiJsonAdapter(model, new TextAdapter.Config(config.properties: _*))

}
