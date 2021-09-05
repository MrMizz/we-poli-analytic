package in.tap.we.poli.analytic.jobs.dynamo.traversal.n1

import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.dynamo.traversal.n1.GraphTraversalSB1Job.sortBy
import in.tap.we.poli.analytic.jobs.dynamo.traversal.nx.NxJob
import in.tap.we.poli.analytic.jobs.dynamo.traversal.nx.NxKey.N1Key
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.Analytics
import org.apache.spark.sql.SparkSession

/**
 * Paginated Graph Traversals
 * Sorted By => Number of Total Transactions (num_edges).
 */
class GraphTraversalSB1Job(
  override val inArgs: OneInArgs,
  override val outArgs: OneOutArgs
)(
  implicit
  override val spark: SparkSession
) extends NxJob[N1Key](
      inArgs,
      outArgs,
      sortBy
    )

object GraphTraversalSB1Job {

  def sortBy(analytics: Analytics): Option[Double] = {
    Some(analytics.num_edges.toDouble)
  }

}
