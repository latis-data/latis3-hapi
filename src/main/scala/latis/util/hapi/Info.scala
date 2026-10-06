package latis.util.hapi

import io.circe.Decoder

/** Response from a HAPI `info` endpoint. */
final case class Info(
  startDate: String,
  stopDate: String,
  parameters: List[Parameter]
)

object Info {
  def infoDecoder(quirks: Quirks): Decoder[Info] = 
    given paramDecoder: Decoder[Parameter] = Parameter.decodeParameter(quirks: Quirks)

    Decoder.forProduct3("startDate", "stopDate", "parameters")(Info.apply)
}
