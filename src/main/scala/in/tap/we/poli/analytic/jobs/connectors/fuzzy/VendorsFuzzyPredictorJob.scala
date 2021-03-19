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
      -15.255417055051492
    }

    private val COEFFICIENTS: Features = {
      Features(
        numTokens = -4.171000822353506,
        numTokensInCommon = 4.829195823531964,
        numEdgesInCommon = 10.659892922847764,
        sameCity = 2.490054780664544,
        sameZip = 2.3178939814664297,
        sameState = 3.8327633735973827
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
