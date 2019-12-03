package latis.input

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.{ByteBuffer, ByteOrder}

import cats.effect.IO
import fs2.{Pipe, Stream}
import latis.data.{Data, Sample}
import latis.model.{DataType, Function, Scalar}
import scodec.codecs._
import scodec.{Decoder, bits}
import scodec.stream.decode.StreamDecoder
import scodec.stream._

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
    StreamSource.getStream(uri).through(decodeToSample)
//    StreamSource.getStream(uri)
//      .chunkN(blockSize)
//      .map(_.toArray)

  private val decodeToSample: Pipe[IO, Byte, Sample] = 
    StreamDecoder.many(makeDecoder(model)).toPipeByte

  /**
   * Makes a Decoder given the dataset's model.
   * Integers are always signed and four byte.
   */
  private def makeDecoder(model: DataType): Decoder[Sample] = {
    ???

    model.getScalars.map { s =>
      s("type") match {
        case Some("int")    => int32 map { i => Data.IntValue(i) }
        case Some("double") => doubleL map { d => Data.DoubleValue(d) } //Data.DoubleValue(buffer.getDouble)
        case Some("string") =>
          val length = bytesToRead(s) * 8
          variableSizeBytes(uint(length), utf8) map { s => Data.StringValue(s) }
        case Some(_) => ??? //unsupported type
        case None => ??? //type not defined
      }
    }
    
    //go through scalars, produce Decorder[Data] for each of them, then have List(s) of Decoder(s) of Data (1 or 2 depending on when I split into domain/range), then
    //I want Decoder[List[Data]]
    //TODO: I'll want/need scodec-cats for Sequence;
    
    //---idea------
    //Map through model's scalars gets me List[Decoder[Data]],
    //with that list x, x.sequence;
    //that gets me Decoder[List[Data]]
    //in that Decoder, to partition list of data into domain/range,[siq] map the Sample constructor over all that to end up with Decoder[Sample]
    //(I might be able to use Traverse instead of Sequence; it would be preferable--because it's equal to a sequence and a map)
    //TODO: thisDecoder.many should give me StreamDecoder[Sample] ...
            //one thing that could be done: I can turn a Chunk (using the approach where I chunked by record sizes) into a BitVector, then rather than using stream decoder stuff, I could use decoder on BitVector
    

  }
  
  
  
  
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
  private def extractData(record: Array[Byte]): List[Data] = {
    val buffer = ByteBuffer.wrap(record).order(order)
    
    model.getScalars.map { s => 
      s("type") match {
        case Some("int")    => Data.IntValue(buffer.getInt)
        case Some("double") => Data.DoubleValue(buffer.getDouble)
        case Some("string") => 
          val length = bytesToRead(s)
          val bytes = new Array[Byte](length)
          buffer.get(bytes)
          Data.StringValue(new String(bytes, StandardCharsets.UTF_8))
        case Some(_) => ??? //unsupported type
        case None => ??? //type not defined
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
    s("type") match {
      case Some("int")    => 4
      case Some("double") => 8
      case Some("string") => s("length") match {
        case Some(l) => l.toInt //TODO: this conversion can fail
        case None => ??? //cannot determine number of bytes to read
      }
      case Some(_) => ??? //unsupported type
      case None => ??? //type not defined
    }
  }
}
