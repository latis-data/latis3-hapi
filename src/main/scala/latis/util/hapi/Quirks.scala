package latis.util.hapi

/**
  * Represents deviations from the HAPI spec in datasets
  *
  * @param optionalLength For string variables, if the `length` attribute in the /info response is optional
  */
case class Quirks(optionalLength: Boolean)

object Quirks {
    /** No deviations from the HAPI spec */
    def strict = Quirks(optionalLength = false)
}
