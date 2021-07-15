package latis.input

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.{ByteBuffer, ByteOrder}

import cats.effect.IO
import fs2.Stream

import latis.data._
import latis.model._

/**
 * Adapter for parsing HAPI binary data.
 */
class BinaryAdapter(model: DataType) extends StreamingAdapter[Array[Byte]] {
  
  val order = ByteOrder.LITTLE_ENDIAN //all numeric values are little endian (LSB)
  
  lazy val blockSize: Int = model.getScalars.map(bytesToRead).sum

  /**
   * Provides a Stream of records as Arrays of bytes.
   */
  def recordStream(uri: URI): Stream[IO, Array[Byte]] =
    StreamSource.getStream(uri)
      .chunkN(blockSize)
      .map(_.toArray)
  
  /**
   * Parses a record into a Sample. 
   * Returns None if the record is invalid.
   */
  def parseRecord(record: Array[Byte]): Option[Sample] = {
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
    // and split into Vectors of domain and range.
    val data = extractData(record)
    val (ds, rs) = data.splitAt(dtypes.length)

    // Construct a Sample from the domain and range values.
    if (rtypes.length != rs.length) None //invalid record
    else 
      Some(Sample(ds, rs))
  }

  /**
   * Extracts the data values from the given record as a List.
   */
  private def extractData(record: Array[Byte]): List[Datum] = {
    val buffer = ByteBuffer.wrap(record).order(order)
    
    model.getScalars.map { s => 
      s.valueType match {
        case IntValueType    => Data.IntValue(buffer.getInt)
        case DoubleValueType => Data.DoubleValue(buffer.getDouble)
        case StringValueType =>
          val length = bytesToRead(s)
          val bytes = new Array[Byte](length)
          buffer.get(bytes)
          Data.StringValue(new String(bytes, StandardCharsets.UTF_8))
        case _ => ??? //unsupported type
      }
    }
  }

  /**
   * Gets the number of bytes to read for the given scalar
   * according to the HAPI specification.
   * 
   * Parameters of type string and isotime have a "length" 
   * specified in the info header that indicates how many bytes 
   * to read for each string value.
   */
  private def bytesToRead(s: Scalar): Int = {
    s.valueType match {
      case IntValueType    => 4
      case DoubleValueType => 8
      case StringValueType => s.metadata.getProperty("length") match { //TODO: "size"?
        case Some(l) => l.toInt //TODO: this conversion can fail
        case None => ??? //cannot determine number of bytes to read
      }
      case _ => ??? //unsupported type
    }
  }
}
