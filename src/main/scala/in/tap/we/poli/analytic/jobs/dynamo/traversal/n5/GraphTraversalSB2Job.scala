package in.tap.we.poli.analytic.jobs.dynamo.traversal.n5

import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.dynamo.traversal.nx.NxJob
import in.tap.we.poli.analytic.jobs.dynamo.traversal.nx.NxKey.N5Key
import org.apache.spark.sql.SparkSession

/**
 * Paginated Graph Traversals
 * Sorted By => Total Spend.
 */
class GraphTraversalSB2Job(
  override val inArgs: OneInArgs,
  override val outArgs: OneOutArgs
)(
  implicit
  override val spark: SparkSession
) extends NxJob[N5Key](
      inArgs,
      outArgs,
      _.total_spend
    )
