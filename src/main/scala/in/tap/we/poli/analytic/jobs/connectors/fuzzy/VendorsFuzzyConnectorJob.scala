package in.tap.we.poli.analytic.jobs.connectors.fuzzy

import in.tap.we.poli.analytic.jobs.connectors.fuzzy.VendorsFuzzyConnectorFeaturesJob.{Features, UniqueVendorComparison}

class VendorsFuzzyConnectorJob {}

object VendorsFuzzyConnectorJob {

  final case class Prediction(
    features: Features
  ) {

    private val INTERCEPT: Double = {
      -23.2094090439513
    }

    private val COEFFICIENTS: Features = {
      Features(
        numTokens = -1.523107019783214,
        numTokensInCommon = 1.7897551114091281,
        numEdges = -75.59071441287279,
        numEdgesInCommon = 86.75005147907491,
        sameCity = 3.040397173649981,
        sameZip = 1.5556153159226587,
        sameState = 2.355427278120487
      )
    }

    private val dot: Double = {
      features
        .toArray
        .zip(COEFFICIENTS.toArray)
        .map(
          Function.tupled(_ * _)
        )
        .sum
    }

    private val expMargin: Double = {
      math.exp(INTERCEPT + dot)
    }

    private val sigmoid: Double = {
      expMargin / (1 + expMargin)
    }

  }

  object Prediction {

    def apply(comparison: UniqueVendorComparison): Double = {
      Prediction(comparison.features).sigmoid
    }

    def predict(features: Features): Double = {
      Prediction(features).sigmoid
    }

  }

}
