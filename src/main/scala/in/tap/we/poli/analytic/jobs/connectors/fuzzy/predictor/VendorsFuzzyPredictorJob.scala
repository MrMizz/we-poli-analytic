package in.tap.we.poli.analytic.jobs.connectors.fuzzy.predictor

import in.tap.base.spark.jobs.composite.TwoInOneOutJob
import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.Comparison
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.Features.{
  AddressFeatures, CompositeFeatures, NameFeatures
}
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.VendorsFuzzyConnectorFeaturesJob.SampleBuilder
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.predictor.VendorsFuzzyPredictorJob.FeaturesTuple
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.transfomer.IdResVendorTransformerJob.IdResVendor
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class VendorsFuzzyPredictorJob(val inArgs: TwoInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[IdResVendor],
  val readTypeTagB: universe.TypeTag[(VertexId, VertexId)],
  val writeTypeTagA: universe.TypeTag[(Double, FeaturesTuple, Comparison)]
) extends TwoInOneOutJob[IdResVendor, (VertexId, VertexId), (Double, FeaturesTuple, Comparison)](inArgs, outArgs) {

  override def transform(
    input: (Dataset[IdResVendor], Dataset[(VertexId, VertexId)])
  ): Dataset[(Double, FeaturesTuple, Comparison)] = {
    val (vendors, connector) = {
      input
    }
    import spark.implicits._
    SampleBuilder
      .negatives(vendors, connector)
      .map { comparison: Comparison =>
        (
          Prediction(comparison),
          (comparison.compositeFeatures, comparison.nameFeatures, comparison.addressFeatures),
          comparison
        )
      }
      .toDS
  }

}

object VendorsFuzzyPredictorJob {

  type FeaturesTuple = (CompositeFeatures, NameFeatures, AddressFeatures)

}
