package in.tap.we.poli.analytic.jobs.connectors.fuzzy.predictor

import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.Features.{
  AddressFeatures, CompositeFeatures, NameFeatures, TransactionFeatures
}
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.{Comparison, Features}

sealed abstract class Prediction[F <: Features](features: F) {

  protected val INTERCEPT: Double

  protected val COEFFICIENTS: F

  final private lazy val dot: Double = {
    features
      .toArray
      .zip(COEFFICIENTS.toArray)
      .map(
        Function.tupled(_ * _)
      )
      .sum
  }

  final private lazy val expMargin: Double = {
    math.exp(INTERCEPT + dot)
  }

  final protected lazy val sigmoid: Double = {
    expMargin / (1 + expMargin)
  }

}

object Prediction {

  final case class CompositePrediction(features: CompositeFeatures) extends Prediction[CompositeFeatures](features) {

    override protected val INTERCEPT: Double = {
      -10.279727464750225
    }

    override protected val COEFFICIENTS: CompositeFeatures = {
      CompositeFeatures(
        nameScore = 9.79417768313024,
        addressScore = 9.46038399497283,
        transactionScore = 1.0
      )
    }

  }

  final case class NamePrediction(features: NameFeatures) extends Prediction[NameFeatures](features) {

    override protected val INTERCEPT: Double = {
      -0.9047493027508691
    }

    override protected val COEFFICIENTS: NameFeatures = {
      NameFeatures(
        numTokens = -3.0693857072278545,
        numTokensInCommon = 4.140643147635645
      )
    }

  }

  final case class AddressPrediction(features: AddressFeatures) extends Prediction[AddressFeatures](features) {

    override protected val INTERCEPT: Double = {
      -6.06291023757855
    }

    override protected val COEFFICIENTS: AddressFeatures = {
      AddressFeatures(
        sameZip = 1.1382999227893942,
        sameCity = 2.269141627071912,
        sameState = 3.873981104265902
      )
    }

  }

  final case class TransactionPrediction(features: TransactionFeatures)
      extends Prediction[TransactionFeatures](features) {

    override protected val INTERCEPT: Double = {
      0.0
    }

    override protected val COEFFICIENTS: TransactionFeatures = {
      TransactionFeatures(
        sameSrcId = 1.0,
        reportYearDiff = 1.0,
        sameReportType = 1.0,
        sameFormType = 1.0,
        amountPaidDiffRatio = 1.0,
        sameDisbursementCategory = 1.0,
        sameEntityType = 1.0
      )
    }

  }

  def apply(comparison: Comparison): Double = {
    CompositePrediction(comparison.compositeFeatures).sigmoid
  }

  def apply(features: CompositeFeatures): Double = {
    CompositePrediction(features).sigmoid
  }

  def apply(features: NameFeatures): Double = {
    NamePrediction(features).sigmoid
  }

  def apply(features: AddressFeatures): Double = {
    AddressPrediction(features).sigmoid
  }

  def apply(features: TransactionFeatures): Double = {
    TransactionPrediction(features).sigmoid
  }

}
