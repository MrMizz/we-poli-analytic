package in.tap.we.poli.analytic.jobs.dynamo.traversal.nx

import in.tap.we.poli.analytic.jobs.dynamo.traversal.Traversal.TraversalWithCount
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.Analytics
import org.apache.spark.graphx.VertexId

import scala.math.Numeric.DoubleIsFractional

sealed trait NxTraversalBuilder extends Product {

  protected val arity: Int

  private def key(keys: Seq[String]): String = {
    keys.sorted.reduce[String] {
      case (left, right) =>
        s"${left}_$right"
    }
  }

  /** init */
  def apply(srcId: VertexId, traversals: Seq[(VertexId, Analytics)]): Seq[(String, TraversalWithCount)] = {
    traversals.combinations(arity).toList.map { combination: Seq[(VertexId, Analytics)] =>
      (key(combination.map(_._1.toString)), (Seq((srcId, apply(combination.map(_._2)))), 1L))
    }
  }

  private def apply(analytics: Seq[Analytics]): Analytics = {
    Analytics(
      num_edges = analytics.map(_.num_edges).sum,
      total_spend = toOption(analytics.flatMap(_.total_spend).sum),
      avg_spend = toOption(analytics.flatMap(_.avg_spend).sum),
      min_spend = toOption(analytics.flatMap(_.min_spend).sum),
      max_spend = toOption(analytics.flatMap(_.max_spend).sum)
    )
  }

  private def toOption(d: Double): Option[Double] = {
    if (d == DoubleIsFractional.zero) None else Some(d)
  }

}

object NxTraversalBuilder {

  case object N1TraversalBuilder extends NxTraversalBuilder {
    override protected val arity: Int = 1
  }

  case object N2TraversalBuilder extends NxTraversalBuilder {
    override protected val arity: Int = 2
  }

  case object N3TraversalBuilder extends NxTraversalBuilder {
    override protected val arity: Int = 3
  }

  case object N4TraversalBuilder extends NxTraversalBuilder {
    override protected val arity: Int = 4
  }

  case object N5TraversalBuilder extends NxTraversalBuilder {
    override protected val arity: Int = 5
  }

}
