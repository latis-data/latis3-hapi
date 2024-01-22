package latis.util.hapi

import cats.syntax.all.*
import munit.CatsEffectSuite
import scodec.Attempt
import scodec.Codec.given
import scodec.DecodeResult
import scodec.Encoder as SEncoder
import scodec.bits.*
import scodec.interop.cats.*

import latis.data.Data.*
import latis.dataset.Dataset
import latis.dsl.DatasetGenerator
import latis.metadata.Metadata
import latis.model.*
import latis.output.BinaryEncoder
import latis.util.Identifier.*

class HapiBinaryEncoderSuite extends CatsEffectSuite {

  /** Instance of BinaryEncoder for testing. */
  val enc: BinaryEncoder = new BinaryEncoder(DataCodec.hapiCodec)

  test("encode a dataset to binary") {
    val ds: Dataset = DatasetGenerator("x -> (a: int, b: double)")
    val encoded = enc.encode(ds).compile.to(ByteVector).map(_.bits)

    val expected = List(
      SEncoder.encode(0),
      SEncoder.encode(0),
      SEncoder.encode(0.0),
      SEncoder.encode(1),
      SEncoder.encode(1),
      SEncoder.encode(1.0),
      SEncoder.encode(2),
      SEncoder.encode(2),
      SEncoder.encode(2.0),
    ).foldMap(_.require.reverseByteOrder)

    encoded.assertEquals(expected)
  }

  test("encode ints as 32-bit little-endian integers") {
    val d = IntValue(3)
    val s = Scalar(id"a", IntValueType)

    assertEquals(
      DataCodec.hapiCodec(s).encode(d),
      Attempt.successful(hex"03000000".bits)
    )
  }

  test("encode doubles as 64-bit little-endian doubles") {
    val d = DoubleValue(6.6)
    val s = Scalar(id"a", DoubleValueType)

    assertEquals(
      DataCodec.hapiCodec(s).encode(d),
      Attempt.successful(hex"6666666666661a40".bits)
    )
  }

  test("encode strings as null-padded ASCII") {
    val d = StringValue("foo")
    val s = Scalar.fromMetadata(
      Metadata(
        "id" -> "a",
        "type" -> "string",
        "size" -> "5"
      )
    ).getOrElse(fail("Scalar not generated"))

    assertEquals(
      DataCodec.hapiCodec(s).encode(d),
      Attempt.successful(hex"666f6f0000".bits)
    )
  }

  test("decode ints from 32-bit little-endian integers") {
    val d = IntValue(3)
    val s = Scalar(id"a", IntValueType)

    assertEquals(
      DataCodec.hapiCodec(s).decode(hex"03000000".bits),
      Attempt.successful(DecodeResult(d, BitVector.empty))
    )
  }

  test("decode doubles from 64-bit little-endian doubles") {
    val d = DoubleValue(6.6)
    val s = Scalar(id"a", DoubleValueType)

    assertEquals(
      DataCodec.hapiCodec(s).decode(hex"6666666666661a40".bits),
      Attempt.successful(DecodeResult(d, BitVector.empty))
    )
  }

  test("decode strings from null-padded ASCII") {
    val d = StringValue("foo")
    val s = Scalar.fromMetadata(
      Metadata(
        "id" -> "a",
        "type" -> "string",
        "size" -> "5"
      )
    ).getOrElse(fail("Scalar not generated"))

    assertEquals(
      DataCodec.hapiCodec(s).decode(hex"666f6f0000".bits),
      Attempt.successful(DecodeResult(d, BitVector.empty))
    )
  }
}
