package in.tap.we.poli.analytic.jobs.dynamo.traversal

import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.dynamo.traversal.GraphTraversalJob.GraphTraversal.TraversalWithCount
import in.tap.we.poli.analytic.jobs.dynamo.traversal.GraphTraversalSB5Job.sortBy
import org.apache.spark.sql.SparkSession

/**
 * Paginated Graph Traversals
 * Sorted By => Maximum Spend.
 */
class GraphTraversalSB5Job(
  override val inArgs: OneInArgs,
  override val outArgs: OneOutArgs
)(
  implicit
  override val spark: SparkSession
) extends GraphTraversalJob(
      inArgs,
      outArgs,
      sortBy
    )

object GraphTraversalSB5Job {

  def sortBy(traversalWithCount: TraversalWithCount): TraversalWithCount = {
    traversalWithCount match {
      case (traversal, count) =>
        (traversal.sortBy(_._2.max_spend).reverse, count)
    }
  }

}
