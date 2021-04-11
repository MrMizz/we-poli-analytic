package in.tap.we.poli.analytic.jobs.connectors.fuzzy.predictor

import in.tap.base.spark.jobs.composite.TwoInOneOutJob
import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.VendorsFuzzyConnectorFeaturesJob.{
  Comparison, Features, SampleBuilder
}
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.predictor.VendorsFuzzyPredictorJob.Prediction
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.transfomer.IdResVendorTransformerJob.IdResVendor
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class VendorsFuzzyPredictorJob(val inArgs: TwoInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[IdResVendor],
  val readTypeTagB: universe.TypeTag[(VertexId, VertexId)],
  val writeTypeTagA: universe.TypeTag[(Double, Features, Comparison)]
) extends TwoInOneOutJob[IdResVendor, (VertexId, VertexId), (Double, Features, Comparison)](inArgs, outArgs) {

  override def transform(
    input: (Dataset[IdResVendor], Dataset[(VertexId, VertexId)])
  ): Dataset[(Double, Features, Comparison)] = {
    val (vendors, connector) = {
      input
    }
    import spark.implicits._
    SampleBuilder
      .negatives(vendors, connector)
      .map { comparison: Comparison =>
        (Prediction(comparison), comparison.features, comparison)
      }
      .toDS
  }
}

object VendorsFuzzyPredictorJob {

  final case class Prediction(
    features: Features
  ) {

    private val INTERCEPT: Double = {
      -4.68897505832906
    }

    private val COEFFICIENTS: Features = {
      Features(
        numTokens = -2.735514394166897,
        numTokensInCommon = 3.0781215618268343,
        sameSrcId = 0.2289390859890858,
        sameZip = 1.273224159572643,
        sameCity = 2.3260977265320752,
        sameState = 4.55193095977757
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
