package in.tap.we.poli.analytic.jobs.dynamo.traversal

import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.TwoOutArgs
import in.tap.we.poli.analytic.jobs.dynamo.traversal.GraphTraversalJob.GraphTraversal.TraversalWithCount
import in.tap.we.poli.analytic.jobs.dynamo.traversal.GraphTraversalSB1Job.sortBy
import org.apache.spark.sql.SparkSession

/**
 * Paginated Graph Traversals
 * Sorted By => Average Spend.
 */
class GraphTraversalSB3Job(
  override val inArgs: OneInArgs,
  override val outArgs: TwoOutArgs
)(
  implicit
  override val spark: SparkSession
) extends GraphTraversalJob(
      inArgs,
      outArgs,
      sortBy
    )

object GraphTraversalSB3Job {

  def sortBy(traversalWithCount: TraversalWithCount): TraversalWithCount = {
    traversalWithCount match {
      case (traversal, count) =>
        (traversal.sortBy(_._2.avg_spend).reverse, count)
    }
  }

}
