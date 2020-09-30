package in.tap.we.poli.analytic.jobs.connectors.fuzzy

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.VendorsFuzzyConnectorFeaturesJob.{
  Comparator, Features, UniqueVendorComparison
}
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.VendorsFuzzyConnectorJob.Prediction
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class VendorsFuzzyConnectorJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[UniqueVendor],
  val writeTypeTagA: universe.TypeTag[(UniqueVendorComparison, Double)]
) extends OneInOneOutJob[UniqueVendor, (UniqueVendorComparison, Double)](inArgs, outArgs) {

  override def transform(input: Dataset[UniqueVendor]): Dataset[(UniqueVendorComparison, Double)] = {
    import spark.implicits._
    input
      .flatMap { uniqueVendor =>
        val comparator: Comparator[UniqueVendor] = Comparator(uniqueVendor)
        comparator.nameTokens.map { token: String =>
          token -> Option(Seq(comparator))
        }
      }
      .rdd
      .reduceByKey(VendorsFuzzyConnectorFeaturesJob.reduceCandidates)
      .flatMap {
        case (_, candidates: Option[Seq[Comparator[UniqueVendor]]]) =>
          candidates match {
            case None => Nil
            case Some(seq) =>
              UniqueVendorComparison(seq).map { uniqueVendorComparison: UniqueVendorComparison =>
                uniqueVendorComparison -> Prediction(uniqueVendorComparison)
              }
          }
      }
      .toDS
  }

}

object VendorsFuzzyConnectorJob {

  final case class Prediction(
    features: Features
  ) {

    private val INTERCEPT: Double = {
      -10.214223321792378
    }

    private val COEFFICIENTS: Features = {
      Features(
        numTokens = -3.6251205573716705,
        numTokensInCommon = 3.971440290285166,
        numEdgesInCommon = 5.466347141453324,
        sameCity = 3.294034671172241,
        sameZip = 2.1823695306212847,
        sameState = 3.9277953318427277
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
