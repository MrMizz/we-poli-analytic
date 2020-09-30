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
      -13.303282134614815
    }

    private val COEFFICIENTS: Features = {
      Features(
        numTokens = -3.582353931988716,
        numTokensInCommon = 3.9472231658028534,
        numEdges = -69.12672651523857,
        numEdgesInCommon = 77.9834809981517,
        sameCity = 3.002297935251703,
        sameZip = 2.29705188515646,
        sameState = 4.220344714202386
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
