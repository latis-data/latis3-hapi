package latis.input

import java.net.URI
import java.nio.{ByteBuffer, ByteOrder}

import cats.effect.IO
import fs2.Stream
import latis.data.Sample
import latis.model.{DataType, Function, Scalar}

import scala.collection.mutable.ArrayBuffer

/**
 * Adapter for HAPI binary datasets.
 */
class HapiBinaryAdapter(model: DataType) extends StreamingAdapter[Iterator[Byte]] {
  
  val order = ByteOrder.LITTLE_ENDIAN //all numeric values are little endian (LSB)
  
  lazy val blockSize: Int = model.getScalars.map(getSizeInBytes).sum

  /**
   * Parameters of type string and isotime have a "length" 
   * specified in the info header that indicates how many bytes 
   * to read for each string value. 
   * (If the string content is less than the length, 
   * the remaining bytes are padded with ASCII null bytes.)
   */
  lazy val stringLength: Int = 24 //TODO: get this from the info header (i.e. "length" metadata)
  
  /**
   * Gets the number of bytes to read for the given scalar
   * according to the HAPI specification.
   * Note that some values deviate from the number of bytes
   * needed to represent the corresponding primitive type.
   */
  def getSizeInBytes(s: Scalar): Int = {
    s("type") match {
      //case Some("char")   => 2
      case Some("short")  => 2
      case Some("int")    => 8 //not 4 because "four byte and floating point values are always IEEE 754 double precision values"
      case Some("long")   => 8
      case Some("float")  => 8 //not 4 because "four byte and floating point values are always IEEE 754 double precision values"
      case Some("double") => 8
      case Some("string") => stringLength
      case Some(_) => ??? //unsupported type
      case None => ??? //type not defined
    }
  }

  /**
   * Provides a Stream of records as Iterators of bytes.
   */
  def recordStream(uri: URI): Stream[IO, Iterator[Byte]] =
    StreamSource.getStream(uri)
      .chunkN(blockSize)
      .map(_.iterator)
  
  
  /**
   * Parses a record into a Sample. 
   * Returns None if the record is invalid.
   */
  def parseRecord(record: Iterator[Byte]): Option[Sample] = {
    // We assume one value for each scalar in the model.
    // Note that Samples don't capture nested tuple structure.
    // Assume uncurried model (no nested function), for now.
    /*
     * TODO: Traverse the model to build nested functions (Function in range) 
     * should we apply currying logic or require operation? 
     * curry should be cheap for ordered data
     */

    // Get Vectors of domain and range Scalar types.
    val (dtypes, rtypes) = model match {
      case Function(d, r) => (d.getScalars, r.getScalars)
      case _ => (Vector.empty, model.getScalars)
    }

    // Extract the data values from the record
    // and split into Vectors of domain and range values.
    val values = extractValues(record)
    val (dvals, rvals) = values.splitAt(dtypes.length)

    // Zip the types with the values, then construct a Sample 
    // from the parsed domain and range values.
    if (rtypes.length != rvals.length) None //invalid record
    else {
      val ds = (dtypes zip dvals).map(p => p._1.parseValue(p._2))
      val rs = (rtypes zip rvals).map(p => p._1.parseValue(p._2))
      Some(Sample(ds, rs))
    }
  }

  /**
   * Extracts the data values from the given record
   * as a Vector of strings.
   * 
   * TODO: consider a refactor to avoid intermediate string representations
   */
  def extractValues(record: Iterator[Byte]): Vector[String] = {
    val vals = new ArrayBuffer[String]
    
    model.getScalars.map { s => 
      s("type") match {
        case Some("string") => vals += record.take(stringLength).toArray.map(_.toChar).mkString
        case _ => vals += ByteBuffer.wrap(record.take(getSizeInBytes(s)).toArray).order(order).getDouble.toString //numeric or unsupported type
        case None => ??? //type not defined
      }
    }

    vals.toVector
  }
}

//=============================================================================

object HapiBinaryAdapter extends AdapterFactory {

  def apply(model: DataType): HapiBinaryAdapter =
    new HapiBinaryAdapter(model)

  /**
   * Constructor used by the AdapterFactory.
   */
  def apply(model: DataType, config: AdapterConfig): HapiBinaryAdapter =
    new HapiBinaryAdapter(model)

}
