package latis.util.hapi

import munit.CatsEffectSuite
import org.scalatest.EitherValues._
import scodec.bits._
import scodec.codecs.implicits._
import scodec.{Encoder => SEncoder}

import latis.data.Data._
import latis.dataset.Dataset
import latis.dsl.DatasetGenerator
import latis.metadata.Metadata
import latis.model._
import latis.output.BinaryEncoder
import latis.util.Identifier.IdentifierStringContext

class HapiBinaryEncoderSuite extends CatsEffectSuite {

  /** Instance of BinaryEncoder for testing. */
  val enc: BinaryEncoder = new BinaryEncoder(DataCodec.hapiCodec)

  test("encode a dataset to binary") {
    val ds: Dataset = DatasetGenerator("x -> (a: int, b: double)")
    val encodedList = enc.encode(ds).compile.toList
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
    encodedList.map { lst =>
      assertEquals(lst, expected)
    }
  }

  test("encode ints as 32-bit little-endian integers with the HAPI data codec") {
    val d = (3: Int): IntValue
    val s = Scalar(id"a", IntValueType)
    val expected = BitVector(hex"03000000")
    DataCodec.hapiCodec(s).encode(d).map { enc =>
      assertEquals(enc, expected)
    }
  }

  test("encode doubles as 64-bit little-endian doubles with the HAPI data codec") {
    val d = (6.6: Double): DoubleValue
    val s = Scalar(id"a", DoubleValueType)
    val expected = BitVector(hex"6666666666661a40")
    DataCodec.hapiCodec(s).encode(d).map { enc =>
      assertEquals(enc, expected)
    }
  }

  test("encode strings as null-padded ASCII with the HAPI data codec") {
    val d = "foo": StringValue
    val s = Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "string", "size" -> "5")).value
    val expected = BitVector(hex"666f6f0000")
    DataCodec.hapiCodec(s).encode(d).map { enc =>
      assertEquals(enc, expected)
    }
  }

  test("decode ints from 32-bit little-endian integers with the HAPI data codec") {
    val d = (3: Int): IntValue
    val s = Scalar(id"a", IntValueType)
    val encoded = BitVector(hex"03000000")
    DataCodec.hapiCodec(s).decode(encoded).map { dec =>
      assertEquals(dec.value, d)
    }
  }

  test("decode doubles from 64-bit little-endian doubles with the HAPI data codec") {
    val d = (6.6: Double): DoubleValue
    val s = Scalar(id"a", DoubleValueType)
    val encoded = BitVector(hex"6666666666661a40")
    DataCodec.hapiCodec(s).decode(encoded).map { dec =>
      assertEquals(dec.value, d)
    }
  }

  test("decode strings from null-padded ASCII with the HAPI data codec") {
    val d = "foo": StringValue
    val s = Scalar.fromMetadata(Metadata("id" -> "a", "type" -> "string", "size" -> "5")).value
    val encoded = BitVector(hex"666f6f0000")
    DataCodec.hapiCodec(s).decode(encoded).map { dec =>
      assertEquals(dec.value, d)
    }
  }
}
