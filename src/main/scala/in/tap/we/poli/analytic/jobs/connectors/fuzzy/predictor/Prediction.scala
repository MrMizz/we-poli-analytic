package in.tap.we.poli.analytic.jobs.connectors.fuzzy.predictor

import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.Features.{
  AddressFeatures, CompositeFeatures, NameFeatures
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
      0.0
    }

    override protected val COEFFICIENTS: CompositeFeatures = {
      CompositeFeatures(
        sameSrcId = 1.0,
        nameScore = 1.0,
        addressScore = 1.0
      )
    }

  }

  final case class NamePrediction(features: NameFeatures) extends Prediction[NameFeatures](features) {

    override protected val INTERCEPT: Double = {
      0.0
    }

    override protected val COEFFICIENTS: NameFeatures = {
      NameFeatures(
        numTokens = -1.0,
        numTokensInCommon = 1.0
      )
    }

  }

  final case class AddressPrediction(features: AddressFeatures) extends Prediction[AddressFeatures](features) {

    override protected val INTERCEPT: Double = {
      0.0
    }

    override protected val COEFFICIENTS: AddressFeatures = {
      AddressFeatures(
        sameZip = 1.0,
        sameCity = 1.0,
        sameState = 1.0
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

}
