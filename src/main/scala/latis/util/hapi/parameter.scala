package latis.util.hapi

import io.circe.Decoder
import io.circe.DecodingFailure
import io.circe.HCursor

/**
 * Base type for HAPI parameter information.
 *
 * These types encode parameter information as LaTiS consumes it and
 * are not meant to reflect the exact response from a HAPI service.
 */
sealed trait Parameter

/** Represents a parameter without size or bins. */
final case class ScalarParameter(
  name: String,
  typeName: String,
  units: String,
  length: Option[Int],
  fill: Option[String]
) extends Parameter

/** Represents a parameter with size but without bins. */
final case class VectorParameter(
  name: String,
  typeName: String,
  units: String,
  length: Option[Int],
  fill: Option[String],
  size: Int
) extends Parameter

/** Represents a parameter with size and bins. */
final case class ArrayParameter(
  name: String,
  typeName: String,
  units: String,
  length: Option[Int],
  fill: Option[String],
  size: Int,
  bin: Bin
) extends Parameter

object Parameter {

  implicit val decodeParameter: Decoder[Parameter] = new Decoder[Parameter] {
    final def apply(c: HCursor): Decoder.Result[Parameter] = for {
      name   <- c.get[String]("name")
      tyName <- c.get[String]("type")
      units  <- c.get[String]("units")
      // Only time and string parameters have length.
      length <- if (tyName == "string" || tyName == "isotime") {
        c.get[Int]("length").map(Option(_))
      } else Right(None)
      fill   <- c.get[Option[String]]("fill")
      // We only support a single dimension if size is defined.
      size   <- c.get[Option[List[Int]]]("size").flatMap {
        case Some(s :: Nil) => Right(Option(s))
        case None           => Right(None)
        case _              => Left(DecodingFailure("Size", c.history))
      }
      // There will only be bins if size was defined. We only support
      // a single bin if any are defined.
      bin   <- size match {
        case Some(_) => c.get[Option[List[Bin]]]("bins").flatMap {
          case Some(b :: Nil) => Right(Option(b))
          case None           => Right(None)
          case _              => Left(DecodingFailure("Bins", c.history))
        }
        case None    => Right(None)
      }
      param <- bin match {
        case Some(bin) => size match {
          case Some(size) => Right(ArrayParameter(name, tyName, units, length, fill, size, bin))
          case None       => Left(DecodingFailure("Parameter", c.history))
        }
        case None => size match {
          case Some(size) => Right(VectorParameter(name, tyName, units, length, fill, size))
          case None       => Right(ScalarParameter(name, tyName, units, length, fill))
        }
      }
    } yield param
  }
}
