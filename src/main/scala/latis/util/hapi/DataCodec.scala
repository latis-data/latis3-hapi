package latis.util.hapi

import scodec.*
import scodec.bits.*

import latis.data.Data
import latis.data.Data.*
import latis.model.*

/**
 * The HAPI standard only supports 32-bit little endian integers, little endian
 * double-precision floats, and length-specified null-padded ASCII strings. For a
 * LaTiS-based hapiCodec f:Scalar => Codec[Data], this means that only Scalars of
 * IntValueType, DoubleValueType, and StringValueType are supported, with the numeric
 * ones being encoded using little endian codecs, and strings being encoded using the
 * ascii codec with null padding based on the size provided in the Scalar metadata.
 */
object DataCodec {

  /*
   * TODO: replace codecs.fail with Either & LatisExceptions to make failure
   *  happen before a response is sent from the server
   */
  def hapiCodec(s: Scalar): Codec[Data] = s.valueType match {
    case BooleanValueType => codecs.fail(Err("Boolean not supported."))
    case ByteValueType    => codecs.fail(Err("Byte not supported."))
    case ShortValueType   => codecs.fail(Err("Short not supported."))
    case IntValueType     => codecs.int32L.xmap[IntValue](IntValue, _.value).upcast[Data]
    case LongValueType    => codecs.fail(Err("Long not supported."))
    case FloatValueType   => codecs.fail(Err("Float not supported."))
    case DoubleValueType  => codecs.doubleL.xmap[DoubleValue](DoubleValue, _.value).upcast[Data]
    case StringValueType if s.metadata.getProperty("size").nonEmpty =>
      val size = s.metadata.getProperty("size").get.toLong * 8
      codecs.paddedFixedSizeBits(
        size,
        codecs.utf8,
        codecs.constant(BitVector(hex"00"))
      ).xmap[StringValue](s => StringValue(s.replace("\u0000", "")), _.value).upcast[Data]
    case StringValueType  => codecs.fail(Err("String without a size defined not supported."))
    case BinaryValueType  => codecs.fail(Err("Binary blob not supported."))
    //Note that there is no standard binary encoding for Char, BigInt, or BigDecimal
    case CharValueType       => codecs.fail(Err("Char not supported."))
    case BigIntValueType     => codecs.fail(Err("BigInt not supported."))
    case BigDecimalValueType => codecs.fail(Err("BigDecimal not supported."))
  }

}
