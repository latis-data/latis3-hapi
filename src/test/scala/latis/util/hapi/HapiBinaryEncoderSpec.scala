package latis.util.hapi

import cats.effect.unsafe.implicits.global
import org.scalatest.EitherValues._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import scodec.bits._
import scodec.codecs.implicits._
import scodec.{Encoder => SEncoder, _}

import latis.data.Data._
import latis.dataset.Dataset
import latis.dsl.DatasetGenerator
import latis.metadata.Metadata
import latis.model._
import latis.output.BinaryEncoder
import latis.util.Identifier.IdentifierStringContext

class HapiBinaryEncoderSpec extends AnyFlatSpec {

  /** Instance of BinaryEncoder for testing. */
  val enc: BinaryEncoder = new BinaryEncoder(DataCodec.hapiCodec)

  "A Binary encoder" should "encode a dataset to binary" in {
    val ds: Dataset = DatasetGenerator("x -> (a: int, b: double)")
    val encodedList = enc.encode(ds).compile.toList.unsafeRunSync()

    val bitVec =
      SEncoder.encode(0).require.reverseByteOrder ++
        SEncoder.encode(0).require.reverseByteOrder ++
        SEncoder.encode(0.0).require.reverseByteOrder ++
      SEncoder.encode(1).require.reverseByteOrder ++
        SEncoder.encode(1).require.reverseByteOrder ++
        SEncoder.encode(1.0).require.reverseByteOrder ++
      SEncoder.encode(2).require.reverseByteOrder ++
        SEncoder.encode(2).require.reverseByteOrder ++
        SEncoder.encode(2.0).require.reverseByteOrder

    val expected = bitVec.toByteArray.toList

    encodedList should be(expected)
  }

  "The HAPI data codec's encoder" should "encode ints as 32-bit little-endian integers" in {
    val d = (3: Int): IntValue
    val s = Scalar(id"a", IntValueType)
    val expected = Attempt.successful(BitVector(hex"03000000"))
    DataCodec.hapiCodec(s).encode(d) should be(expected)
  }

  it should "encode doubles as 64-bit little-endian doubles" in {
    val d = (6.6: Double): DoubleValue
    val s = Scalar(id"a", DoubleValueType)
    val expected = Attempt.successful(BitVector(hex"6666666666661a40"))
    DataCodec.hapiCodec(s).encode(d) should be(expected)
  }

  it should "encode strings as null-padded ASCII" in {
    val d = "foo": StringValue
    val s = Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "string", "size" -> "5")).value
    val expected = Attempt.successful(BitVector(hex"666f6f0000"))
    DataCodec.hapiCodec(s).encode(d) should be(expected)
  }

  "The HAPI data codec's decoder" should "decode ints from 32-bit little-endian integers" in {
    val d = (3: Int): IntValue
    val s = Scalar(id"a", IntValueType)
    val encoded = BitVector(hex"03000000")
    val decoded = DataCodec.hapiCodec(s).decode(encoded) match {
      case Attempt.Successful(DecodeResult(value, _)) => value
      case _ => fail("Failed to decode")
    }
    decoded should be(d)
  }

  it should "decode doubles from 64-bit little-endian doubles" in {
    val d = (6.6: Double): DoubleValue
    val s = Scalar(id"a", DoubleValueType)
    val encoded = BitVector(hex"6666666666661a40")
    val decoded = DataCodec.hapiCodec(s).decode(encoded) match {
      case Attempt.Successful(DecodeResult(value, _)) => value
      case _ => fail("Failed to decode")
    }
    decoded should be(d)
  }

  it should "decode strings from null-padded ASCII" in {
    val d = "foo": StringValue
    val s = Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "string", "size" -> "5")).value
    val encoded = BitVector(hex"666f6f0000")
    val decoded = DataCodec.hapiCodec(s).decode(encoded) match {
      case Attempt.Successful(DecodeResult(value, _)) => value
      case _ => fail("Failed to decode")
    }
    decoded should be(d)
  }
}
