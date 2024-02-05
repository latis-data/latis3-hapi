package latis.util

import scala.io.Source

import io.circe.*
import munit.CatsEffectSuite

/** A base class for testing JSON decoders. */
abstract class JsonDecoderSuite extends CatsEffectSuite {

  /**
   * Parses JSON from a file on the classpath.
   *
   * Cancels the test if the required resource could not be read.
   *
   * @param resource path to resource
   * @param f test to run with parsed JSON
   */
  def withJsonResource(resource: String)(f: Json => Any): Any =
    try {
      parser.parse(Source.fromResource(resource).mkString) match {
        case Right(json) => f(json)
        case Left(error) => fail(error.getMessage)
      }
    } catch {
      case _: NullPointerException =>
        // Not finding a resource manifests as a null pointer.
        fail(s"Unable to find resource: $resource")
    }

  /**
   * Decodes JSON to a given type.
   *
   * Fails the test if decoding fails.
   *
   * @param json JSON to decode
   * @param f test to run with decoded value
   */
  def decodedAs[A: Decoder](json: Json)(f: A => Any): Any =
    Decoder[A].decodeJson(json) match {
      case Right(a) => f(a)
      case Left(error) => fail(error.getMessage)
    }

  /**
   * Checks that JSON does not decode to a given type.
   *
   * Fails the test if JSON successfully decodes.
   *
   * @param json JSON to decode
   * @param msg message to print on failure
   */
  def doesNotDecodeAs[A: Decoder](json: Json)(msg: => String): Any =
    Decoder[A].decodeJson(json) match {
      case Left(_) => assert(true)
      case _ => fail(msg)
    }
}
