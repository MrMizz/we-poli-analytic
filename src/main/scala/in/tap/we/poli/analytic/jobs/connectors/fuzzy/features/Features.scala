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

  final case class TransactionFeatures(
    sameSrcId: Double,
    reportYearDiff: Double,
    sameReportType: Double,
    sameFormType: Double,
    amountPaidDiffRatio: Double,
    sameDisbursementCategory: Double,
    sameEntityType: Double
  ) extends Features {

    override def toArray: Array[Double] = {
      Array(
        sameSrcId,
        reportYearDiff,
        sameReportType,
        sameFormType,
        amountPaidDiffRatio,
        sameDisbursementCategory,
        sameEntityType
      )
    }

  }

}
