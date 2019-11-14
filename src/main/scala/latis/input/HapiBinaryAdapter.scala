package latis.input

import java.net.URI

import cats.effect.IO
import fs2.{Pipe, Stream, text}
import latis.data.Sample
import latis.model.{DataType, Function, Scalar}
import latis.util.{ConfigLike, StreamUtils}

/**
 * Adapter for HAPI binary datasets.
 */
class HapiBinaryAdapter(model: DataType)
  extends StreamingAdapter[Vector[Byte]] {

  /**
   * Parameters of type string and isotime have a "length" 
   * specified in the info header that indicates how many bytes 
   * to read for each string value. 
   * (If the string content is less than the length, 
   * the remaining bytes are padded with ASCII null bytes.)
   */
  def stringLength: Int = 24
  
  lazy val blockSize: Int = model.getScalars.map(getSizeInBytes(_)).sum
  
  def getSizeInBytes(s: Scalar): Int = {
    s("type") match {
      //case Some("char")       => 2
      case Some("short")      => 2
      case Some("int")        => 4
      case Some("long")       => 8
      case Some("float")      => 4
      case Some("double")     => 8
      case Some("string")     => stringLength
      case Some(s) => ??? //unsupported type s
      case None => ??? //type not defined
    }
  }

  /**
   * Provides a Stream of records as Strings.
   * Applies configuration options to the Stream.
   */
  def recordStream(uri: URI): Stream[IO, Vector[Byte]] = {
    
    val testStream = StreamSource.getStream(uri)
      .take(stringLength)
      .through(text.utf8Decode)
    
    val testSeq = StreamUtils.unsafeStreamToSeq(testStream)
    
    println(testSeq)
    
    ???
  }


  /**
   * Parses a record into a Sample. 
   * Returns None if the record is invalid.
   */
  def parseRecord(record: Vector[Byte]): Option[Sample] = {
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
   * Extract the data values from the given record.
   */
  def extractValues(record: Vector[Byte]): Vector[String] =
    ???
  
}

//=============================================================================

//object HapiBinaryAdapter extends AdapterFactory {
//
//  def apply(model: DataType, config: Config = new Config()): HapiBinaryAdapter =
//    new HapiBinaryAdapter(model, config)
//
//  /**
//   * Constructor used by the AdapterFactory.
//   */
//  def apply(model: DataType, config: AdapterConfig): HapiBinaryAdapter =
//    new HapiBinaryAdapter(model, new Config(config.properties: _*))
//
//  /**
//   * Configuration specific to a HapiBinaryAdapter.
//   */
//  class Config(val properties: (String, String)*) extends ConfigLike {
//    val commentCharacter: Option[String] = get("commentCharacter")
//    val delimiter: String                = getOrElse("delimiter", ",")
//    val linesPerRecord: Int              = getOrElse("linesPerRecord", 1)
//    val linesToSkip: Int                 = getOrElse("skipLines", 0)
//    val dataMarker: Option[String]       = get("dataMarker")
//  }
//
//}
