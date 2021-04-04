package in.tap.we.poli.analytic.jobs.connectors.fuzzy

import in.tap.base.spark.graph.ConnectedComponents
import in.tap.base.spark.jobs.composite.TwoInOneOutJob
import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.VendorsFuzzyConnectorJob.{Connector, EdgeBuilder, VertexBuilder}
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.VendorsFuzzyConnectorFeaturesJob.{
  Comparison, SampleBuilder
}
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.predictor.VendorsFuzzyPredictorJob.Prediction
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.transfomer.IdResVendorTransformerJob.IdResVendor
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class VendorsFuzzyConnectorJob(val inArgs: TwoInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[IdResVendor],
  val readTypeTagB: universe.TypeTag[(VertexId, VertexId)],
  val writeTypeTagA: universe.TypeTag[(VertexId, VertexId)]
) extends TwoInOneOutJob[IdResVendor, (VertexId, VertexId), (VertexId, VertexId)](inArgs, outArgs) {

  override def transform(
    input: (Dataset[IdResVendor], Dataset[(VertexId, VertexId)])
  ): Dataset[(VertexId, VertexId)] = {
    val (vendors, connector) = {
      input
    }
    val edges: RDD[Edge[Long]] = {
      SampleBuilder
        .negatives(vendors, connector)
        .flatMap(EdgeBuilder(_))
    }
    val vertices: RDD[(VertexId, VertexId)] = {
      vendors.map(VertexBuilder(_))(writeEncoderA).rdd
    }
    Connector(
      vertices,
      edges
    )
  }
}

object VendorsFuzzyConnectorJob {

  // TODO
  val THRESHOLD: Double = {
    0.90
  }

  object EdgeBuilder {

    def apply(uniqueVendorComparison: Comparison): Option[Edge[Long]] = {
      if (Prediction(uniqueVendorComparison) >= THRESHOLD) {
        Some(
          Edge(
            srcId = uniqueVendorComparison.left_side.vendor.uid,
            dstId = uniqueVendorComparison.right_side.vendor.uid,
            attr = 1L
          )
        )
      } else {
        None
      }
    }

  }

  object VertexBuilder {

    def apply(uniqueVendor: IdResVendor): (VertexId, Long) = {
      uniqueVendor.uid -> 1L
    }

  }

  object Connector {

    def apply(vertices: RDD[(VertexId, VertexId)], edges: RDD[Edge[Long]])(
      implicit spark: SparkSession
    ): Dataset[(VertexId, VertexId)] = {
      import spark.implicits._
      ConnectedComponents(
        vertices,
        edges
      ).toDS()
    }

  }

}
