package in.tap.we.poli.analytic.jobs.dynamo.traversal.nx

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.dynamo.traversal.nx.InitJob.DstId
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.{AggregateExpenditureEdge, Analytics}
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class InitJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[AggregateExpenditureEdge],
  val writeTypeTagA: universe.TypeTag[(VertexId, DstId)]
) extends OneInOneOutJob[AggregateExpenditureEdge, (VertexId, DstId)](inArgs, outArgs) {

  override def transform(input: Dataset[AggregateExpenditureEdge]): Dataset[(VertexId, DstId)] = {
    input.flatMap {
      DstId.apply
    }
  }

}

object InitJob {

  final case class DstId(
    dst_id: VertexId,
    analytics: Analytics
  )

  object DstId {

    def apply(edge: AggregateExpenditureEdge): Seq[(VertexId, DstId)] = {
      Seq(
        edge.src_id -> DstId(edge.dst_id, edge.analytics),
        edge.dst_id -> DstId(edge.src_id, edge.analytics)
      )
    }

    final case class WithCount(
      dst_ids: Seq[DstId],
      count: Long
    )

    object WithCount {

      def reduce(left: WithCount, right: WithCount): WithCount = {
        WithCount(
          left.dst_ids ++ right.dst_ids,
          left.count + right.count
        )
      }

    }

  }

}
