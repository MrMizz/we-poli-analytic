package in.tap.we.poli.analytic.jobs.connectors.fuzzy.predictor

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.VendorsFuzzyConnectorJob.CandidateGenerator
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.VendorsFuzzyConnectorFeaturesJob.{Comparison, Features}
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.predictor.VendorsFuzzyPredictorJob.Prediction
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.transfomer.IdResVendorTransformerJob.IdResVendor
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class VendorsFuzzyPredictorJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[IdResVendor],
  val writeTypeTagA: universe.TypeTag[(Double, Features, Comparison)]
) extends OneInOneOutJob[IdResVendor, (Double, Features, Comparison)](inArgs, outArgs) {

  override def transform(input: Dataset[IdResVendor]): Dataset[(Double, Features, Comparison)] = {
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
      -7.138040405277745
    }

    private val COEFFICIENTS: Features = {
      Features(
        numTokens = -4.646533825717493,
        numTokensInCommon = 4.929290482780092,
        sameSrcId = 0.32040967445587276,
        sameZip = 1.1914401975096889,
        sameCity = 2.7794953009854635,
        sameState = 5.768921167137513
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

    def apply(comparison: Comparison): Double = {
      Prediction(comparison.features).sigmoid
    }

    def predict(features: Features): Double = {
      Prediction(features).sigmoid
    }

  }

}
