package latis.util.hapi

/**
  * Represents deviations from the HAPI spec in datasets
  *
  * @param allowStringWithoutLength For string variables, if the `length` attribute in the /info response is optional
  */
case class Quirks(allowStringWithoutLength: Boolean)

object Quirks {
    /** No deviations from the HAPI spec */
    def strict = Quirks(allowStringWithoutLength = false)
}
