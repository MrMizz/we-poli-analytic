package in.tap.we.poli.analytic.jobs.connectors

object ConnectorUtils {

  private val STOP_WORDS: Set[String] = {
    Set("ltd", "llc", "inc")
  }

  private val PUNCTUATION_REGEX: String = {
    """\p{P}"""
  }

  def cleanedNameTokens(name: String): Seq[String] = {
    name.toLowerCase // to lower case
      .replaceAll(PUNCTUATION_REGEX, "") // strip punctuation
      .filterNot { char: Char =>
        java.lang.Character.isDigit(char) // filter numeric
      }
      .split(" ") // tokenize
      .filterNot { char: String =>
        STOP_WORDS.contains(char) // filter stop words
      }
  }

  def cleanedName(name: String): String = {
    cleanedNameTokens(name).fold("")(_ + _)
  }

}
