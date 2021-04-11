package in.tap.we.poli.analytic.jobs.connectors.fuzzy.features

sealed trait Features {
  def toArray: Array[Double]
}

object Features {

  final case class CompositeFeatures(
    sameSrcId: Double,
    nameScore: Double,
    addressScore: Double
  ) extends Features {

    override def toArray: Array[Double] = {
      Array(
        sameSrcId,
        nameScore,
        addressScore
      )
    }

  }

  final case class NameFeatures(
    numTokens: Double,
    numTokensInCommon: Double
  ) extends Features {

    override def toArray: Array[Double] = {
      Array(
        numTokens,
        numTokensInCommon
      )
    }

  }

  final case class AddressFeatures(
    sameZip: Double,
    sameCity: Double,
    sameState: Double
  ) extends Features {

    def toArray: Array[Double] = {
      Array(
        sameZip,
        sameCity,
        sameState
      )
    }

  }

}
