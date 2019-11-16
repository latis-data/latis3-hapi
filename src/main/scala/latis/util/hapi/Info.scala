package latis.util.hapi

import io.circe.Decoder

/** Response from a HAPI `info` endpoint. */
final case class Info(
  startDate: String,
  stopDate: String,
  parameters: List[Parameter]
)

object Info {
  implicit val infoDecoder: Decoder[Info] =
    Decoder.forProduct3("startDate", "stopDate", "parameters")(Info.apply)
}
