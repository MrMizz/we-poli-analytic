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
      -10.70788036352028
    }

    override protected val COEFFICIENTS: CompositeFeatures = {
      CompositeFeatures(
        nameScore = 9.677693806577869,
        addressScore = 9.987099893193582,
        transactionScore = 1.2641658204667907
      )
    }

  }

  final case class NamePrediction(features: NameFeatures) extends Prediction[NameFeatures](features) {

    override protected val INTERCEPT: Double = {
      1.7326492057241745
    }

    override protected val COEFFICIENTS: NameFeatures = {
      NameFeatures(
        numTokens = -3.9644754241549336,
        numTokensInCommon = 4.729780984275538
      )
    }

  }

  final case class AddressPrediction(features: AddressFeatures) extends Prediction[AddressFeatures](features) {

    override protected val INTERCEPT: Double = {
      -5.433298940120562
    }

    override protected val COEFFICIENTS: AddressFeatures = {
      AddressFeatures(
        sameCity = 2.2614036885291404,
        sameState = 3.807641154215986
      )
    }

  }

  final case class TransactionPrediction(features: TransactionFeatures)
      extends Prediction[TransactionFeatures](features) {

    override protected val INTERCEPT: Double = {
      -1.5349463675370292
    }

    override protected val COEFFICIENTS: TransactionFeatures = {
      TransactionFeatures(
        sameSrcId = 2.9261128664105405,
        reportYearDiff = -0.1589814644507821,
        sameFormType = 0.39738259265256026,
        sameDisbursementCategory = 0.497894548703555,
        sameEntityType = 1.1239140395319738
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
