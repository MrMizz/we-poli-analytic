package in.tap.we.poli.analytic.jobs.dynamo.traversal.n3

import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.dynamo.traversal.GraphTraversalJob
import in.tap.we.poli.analytic.jobs.dynamo.traversal.nx.NxTraversalBuilder.N3TraversalBuilder
import org.apache.spark.sql.SparkSession

/**
 * Paginated Graph Traversals
 * Sorted By => Total Spend.
 */
class GraphTraversalN3SB2Job(
  override val inArgs: OneInArgs,
  override val outArgs: OneOutArgs
)(
  implicit
  override val spark: SparkSession
) extends GraphTraversalJob(
      inArgs,
      outArgs,
      _.total_spend,
      N3TraversalBuilder
    )
