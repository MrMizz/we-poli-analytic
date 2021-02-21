package in.tap.we.poli.analytic.jobs.connectors.fuzzy

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.VendorsFuzzyConnectorFeaturesJob.{Features, UniqueVendorComparison}
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.VendorsFuzzyConnectorJob.CandidateGenerator
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.VendorsFuzzyPredictorJob.Prediction
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class VendorsFuzzyPredictorJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[UniqueVendor],
  val writeTypeTagA: universe.TypeTag[(Double, Features, UniqueVendorComparison)]
) extends OneInOneOutJob[UniqueVendor, (Double, Features, UniqueVendorComparison)](inArgs, outArgs) {

  override def transform(input: Dataset[UniqueVendor]): Dataset[(Double, Features, UniqueVendorComparison)] = {
    import spark.implicits._
    CandidateGenerator(input).map { uniqueVendorComparison =>
      (Prediction(uniqueVendorComparison), uniqueVendorComparison.features, uniqueVendorComparison)
    }.toDS
  }

}

object VendorsFuzzyPredictorJob {

  final case class Prediction(
    features: Features
  ) {

    private val INTERCEPT: Double = {
      -10.100411130863657
    }

    private val COEFFICIENTS: Features = {
      Features(
        numTokens = -4.580928213776146,
        numTokensInCommon = 5.05956615225077,
        numEdgesInCommon = 5.928974677974358,
        sameCity = 2.715578350813287,
        sameZip = 1.4219968861877892,
        sameState = 4.374544337911542
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
