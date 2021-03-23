package in.tap.we.poli.analytic.jobs.connectors.fuzzy

import in.tap.base.spark.graph.ConnectedComponents
import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.VendorsFuzzyConnectorJob.{
  CandidateGenerator, EdgeBuilder, VertexBuilder
}
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.VendorsFuzzyConnectorFeaturesJob.{
  CandidateReducer, Comparator, Comparison
}
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.predictor.VendorsFuzzyPredictorJob.Prediction
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.transfomer.IdResVendorTransformerJob.IdResVendor
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class VendorsFuzzyConnectorJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[IdResVendor],
  val writeTypeTagA: universe.TypeTag[(VertexId, VertexId)]
) extends OneInOneOutJob[IdResVendor, (VertexId, VertexId)](inArgs, outArgs) {

  override def transform(input: Dataset[IdResVendor]): Dataset[(VertexId, VertexId)] = {
    import spark.implicits._
    val edges: RDD[Edge[Long]] = {
      CandidateGenerator(input).flatMap(EdgeBuilder(_))
    }
    val vertices: RDD[(VertexId, VertexId)] = {
      input.map(VertexBuilder(_)).rdd
    }
    ConnectedComponents(
      vertices,
      edges
    ).toDS
  }

}

object VendorsFuzzyConnectorJob {

  // TODO
  val THRESHOLD: Double = {
    0.90
  }

  object CandidateGenerator {

    def apply(uniqueVendors: Dataset[IdResVendor])(implicit spark: SparkSession): RDD[Comparison] = {
      import spark.implicits._
      val comparators: RDD[(String, Option[Seq[Comparator]])] = {
        uniqueVendors.flatMap { uniqueVendor: IdResVendor =>
          val comparator: Comparator = {
            Comparator(uniqueVendor)
          }
          val candidate: Option[Seq[Comparator]] = {
            Option(Seq(comparator))
          }
          comparator.cgTokens.map { token: String =>
            token -> candidate
          }
        }.rdd
      }
      CandidateReducer(comparators)
    }

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

}
