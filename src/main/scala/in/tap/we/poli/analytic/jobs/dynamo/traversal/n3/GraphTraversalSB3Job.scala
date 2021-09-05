package in.tap.we.poli.analytic.jobs.dynamo.traversal.n3

import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.dynamo.traversal.nx.NxJob
import in.tap.we.poli.analytic.jobs.dynamo.traversal.nx.NxKey.N3Key
import org.apache.spark.sql.SparkSession

/**
 * Paginated Graph Traversals
 * Sorted By => Average Spend.
 */
class GraphTraversalSB3Job(
  override val inArgs: OneInArgs,
  override val outArgs: OneOutArgs
)(
  implicit
  override val spark: SparkSession
) extends NxJob[N3Key](
      inArgs,
      outArgs,
      _.avg_spend
    )
