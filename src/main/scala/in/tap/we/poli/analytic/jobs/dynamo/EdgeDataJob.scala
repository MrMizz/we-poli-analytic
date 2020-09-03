package in.tap.we.poli.analytic.jobs.dynamo

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.dynamo.EdgeDataJob.EdgeData
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.AggregateExpenditureEdge
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class EdgeDataJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[AggregateExpenditureEdge],
  val writeTypeTagA: universe.TypeTag[EdgeData]
) extends OneInOneOutJob[AggregateExpenditureEdge, EdgeData](inArgs, outArgs) {

  override def transform(input: Dataset[AggregateExpenditureEdge]): Dataset[EdgeData] = {
    input.map(EdgeData.apply)
  }

}

object EdgeDataJob {

  /**
   * Simplified Edge Data from [[AggregateExpenditureEdge]].
   * 1) We are trimming down the data written to our back end (Dynamo).
   * 2) We are flatting fields to minimize the JSON Codec in our front end (Elm).
   */
  final case class EdgeData(
    src_id: VertexId,
    dst_id: VertexId,
    num_transactions: BigInt,
    total_spend: Option[Double],
    avg_spend: Option[Double],
    min_spend: Option[Double],
    max_spend: Option[Double]
  )

  object EdgeData {

    def apply(aggregateExpenditureEdge: AggregateExpenditureEdge): EdgeData = {
      EdgeData(
        src_id = aggregateExpenditureEdge.src_id,
        dst_id = aggregateExpenditureEdge.dst_id,
        num_transactions = aggregateExpenditureEdge.analytics.num_edges,
        total_spend = aggregateExpenditureEdge.analytics.total_spend,
        avg_spend = aggregateExpenditureEdge.analytics.avg_spend,
        min_spend = aggregateExpenditureEdge.analytics.min_spend,
        max_spend = aggregateExpenditureEdge.analytics.max_spend
      )
    }

  }

}
