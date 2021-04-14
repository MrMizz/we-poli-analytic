package in.tap.we.poli.analytic.jobs.connectors.fuzzy.features

sealed trait Features {
  def toArray: Array[Double]
}

object Features {

  final case class CompositeFeatures(
    nameScore: Double,
    addressScore: Double,
    transactionScore: Double
  ) extends Features {

    override def toArray: Array[Double] = {
      Array(
        nameScore,
        addressScore,
        transactionScore
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
    sameCity: Double,
    sameState: Double
  ) extends Features {

    def toArray: Array[Double] = {
      Array(
        sameCity,
        sameState
      )
    }

  }

  final case class TransactionFeatures(
    sameSrcId: Double,
    reportYearDiff: Double,
    sameFormType: Double,
    sameDisbursementCategory: Double,
    sameEntityType: Double
  ) extends Features {

    override def toArray: Array[Double] = {
      Array(
        sameSrcId,
        reportYearDiff,
        sameFormType,
        sameDisbursementCategory,
        sameEntityType
      )
    }

  }

}
