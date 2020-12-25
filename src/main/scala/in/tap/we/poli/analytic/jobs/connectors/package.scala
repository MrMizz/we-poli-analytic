package in.tap.we.poli.analytic.jobs

import org.apache.spark.graphx.{Edge, VertexId}

package object connectors {

  /** Type Alias for a [[VertexId]] paired to a Connected Component. */
  type Connection = (VertexId, VertexId)

  /**
   * Build Seq of [[Edge]] from group of [[VertexId]].
   *
   * @param seq group identified as same entity
   * @return edges
   */
  def buildEdges(seq: Seq[VertexId]): Seq[Edge[Int]] = {
    seq match {
      case _ :: Nil => Nil
      case head :: tail =>
        tail.map { vertexId: VertexId =>
          Edge(srcId = head, dstId = vertexId, attr = 1)
        }
    }
  }

  /** Tokenize, then clean name tokens. */
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

  /** Clean name token. */
  def cleanedName(name: String): String = {
    cleanedNameTokens(name).fold("")(_ + _)
  }

  /** Stop words, to be removed. */
  private val STOP_WORDS: Set[String] = {
    Set("ltd", "llc", "inc")
  }

  /** Strip punctuation. */
  private val PUNCTUATION_REGEX: String = {
    """\p{P}"""
  }

}
