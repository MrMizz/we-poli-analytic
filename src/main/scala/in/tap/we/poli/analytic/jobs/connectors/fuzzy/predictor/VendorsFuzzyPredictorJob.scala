package in.tap.we.poli.analytic.jobs.connectors.fuzzy.predictor

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.VendorsFuzzyConnectorJob.CandidateGenerator
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.VendorsFuzzyConnectorFeaturesJob.{Comparison, Features}
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.predictor.VendorsFuzzyPredictorJob.Prediction
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.transfomer.IdResVendorTransformerJob
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class VendorsFuzzyPredictorJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[IdResVendorTransformerJob.Source.UniqueVendor],
  val writeTypeTagA: universe.TypeTag[(Double, Features, Comparison)]
) extends OneInOneOutJob[IdResVendorTransformerJob.Source.UniqueVendor, (Double, Features, Comparison)](
      inArgs,
      outArgs
    ) {

  override def transform(
    input: Dataset[IdResVendorTransformerJob.Source.UniqueVendor]
  ): Dataset[(Double, Features, Comparison)] = {
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
      -13.470438671809006
    }

    private val COEFFICIENTS: Features = {
      Features(
        numTokens = -3.5002769364675266,
        numTokensInCommon = 4.554445146317124,
        numSrcIds = -1.236932839134266,
        numSrcIdsInCommon = 3.236932839134266,
        sameCity = 3.008792710911732,
        sameZip = 2.7565359279982005,
        sameState = 3.7694252974252604
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
