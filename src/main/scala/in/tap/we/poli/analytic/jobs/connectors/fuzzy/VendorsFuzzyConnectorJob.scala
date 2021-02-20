package in.tap.we.poli.analytic.jobs.connectors.fuzzy

import in.tap.base.spark.graph.ConnectedComponents
import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.VendorsFuzzyConnectorFeaturesJob.{
  reduceCandidates, Comparator, UniqueVendorComparison
}
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.VendorsFuzzyConnectorJob.{
  CandidateGenerator, EdgeBuilder, VertexBuilder
}
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.VendorsFuzzyPredictorJob.Prediction
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class VendorsFuzzyConnectorJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[UniqueVendor],
  val writeTypeTagA: universe.TypeTag[(VertexId, VertexId)]
) extends OneInOneOutJob[UniqueVendor, (VertexId, VertexId)](inArgs, outArgs) {

  override def transform(input: Dataset[UniqueVendor]): Dataset[(VertexId, VertexId)] = {
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

    def apply(uniqueVendors: Dataset[UniqueVendor])(implicit spark: SparkSession): RDD[UniqueVendorComparison] = {
      import spark.implicits._
      uniqueVendors
        .flatMap { uniqueVendor: UniqueVendor =>
          val comparator: Comparator[UniqueVendor] = Comparator(uniqueVendor)
          val candidate: Option[Seq[Comparator[UniqueVendor]]] = Option(Seq(comparator))
          comparator.nameTokens.map { token: String =>
            token -> candidate
          }
        }
        .rdd
        .reduceByKey(reduceCandidates)
        .flatMap {
          case (_, candidates: Option[Seq[Comparator[UniqueVendor]]]) =>
            candidates match {
              case Some(c) => UniqueVendorComparison(c)
              case None    => Nil
            }
        }
    }

  }

  object EdgeBuilder {

    def apply(uniqueVendorComparison: UniqueVendorComparison): Option[Edge[Long]] = {
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

    def apply(uniqueVendor: UniqueVendor): (VertexId, Long) = {
      uniqueVendor.uid -> 1L
    }

  }

}
