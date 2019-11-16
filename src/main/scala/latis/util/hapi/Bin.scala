package latis.util.hapi

import io.circe.Decoder

/** A bin for a HAPI [[Parameter]]. */
final case class Bin(name: String, units: String)

object Bin {
  implicit val decodeBin: Decoder[Bin] =
    Decoder.forProduct2("name", "units")(Bin.apply)
}
