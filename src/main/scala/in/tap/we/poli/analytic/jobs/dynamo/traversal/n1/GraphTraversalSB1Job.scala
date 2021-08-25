package in.tap.we.poli.analytic.jobs.dynamo.traversal.n1

import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.dynamo.traversal.n1.GraphTraversalJob.GraphTraversal.TraversalWithCount
import in.tap.we.poli.analytic.jobs.dynamo.traversal.n1.GraphTraversalSB1Job.sortBy
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
) extends GraphTraversalJob(
      inArgs,
      outArgs,
      sortBy
    )

object GraphTraversalSB1Job {

  def sortBy(traversalWithCount: TraversalWithCount): TraversalWithCount = {
    traversalWithCount match {
      case (traversal, count) =>
        (traversal.sortBy(_._2.num_edges).reverse, count)
    }
  }

}
