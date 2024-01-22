package latis.util.hapi

import cats.data.NonEmptyList
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
  units: Option[String],
  length: Option[Int],
  fill: Option[String]
) extends Parameter

/** Represents a parameter with size but without bins. */
final case class VectorParameter(
  name: String,
  typeName: String,
  units: Option[String],
  length: Option[Int],
  fill: Option[String],
  size: NonEmptyList[Int]
) extends Parameter

/** Represents a parameter with size and bins. */
final case class ArrayParameter(
  name: String,
  typeName: String,
  units: Option[String], //TODO: could be array of strings
  length: Option[Int],
  fill: Option[String],
  size: NonEmptyList[Int],
  bin: NonEmptyList[Bin]
) extends Parameter

given decodeParameter: Decoder[Parameter] = new Decoder[Parameter] {
  final def apply(c: HCursor): Decoder.Result[Parameter] = for {
    name   <- c.get[String]("name")
    tyName <- c.get[String]("type")
    units  <- c.get[Option[String]]("units")
    // Only time and string parameters have length.
    length <- if (tyName == "string" || tyName == "isotime") {
      c.get[Int]("length").map(Option(_))
    } else Right(None)
    fill   <- c.get[Option[String]]("fill")
    size   <- c.get[Option[NonEmptyList[Int]]]("size").flatMap {
      case Some(s) => Right(Option(s))
      case None    => Right(None)
    }
    // There will only be bins if size was defined.
    bins  <- size match {
      //TODO: make sure bins are consistent with size
      case Some(_) => c.get[Option[NonEmptyList[Bin]]]("bins").flatMap {
        case Some(b) => Right(Option(b))
        case None    => Right(None)
      }
      case None    => Right(None)
    }
    param <- bins match {
      case Some(bins) => size match {
        case Some(size) => Right(ArrayParameter(name, tyName, units, length, fill, size, bins))
        case None       => Left(DecodingFailure("Parameter", c.history))
      }
      case None => size match {
        case Some(size) => Right(VectorParameter(name, tyName, units, length, fill, size))
        case None       => Right(ScalarParameter(name, tyName, units, length, fill))
      }
    }
  } yield param
}
