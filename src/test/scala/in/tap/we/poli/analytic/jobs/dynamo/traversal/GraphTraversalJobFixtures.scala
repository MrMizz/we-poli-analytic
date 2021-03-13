package in.tap.we.poli.analytic.jobs.dynamo.traversal

import in.tap.we.poli.analytic.jobs.dynamo.traversal.GraphTraversalJob.GraphTraversal.TraversalWithCount
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.Analytics

trait GraphTraversalJobFixtures {

  val traversalWithCount1: TraversalWithCount = {
    val analytics = {
      Analytics(num_edges = 19, None, None, None, None)
    }
    (Seq.fill(99)(22L -> analytics), 99)
  }

  val traversalWithCount2: TraversalWithCount = {
    val analytics = {
      Analytics(num_edges = 21, None, None, None, None)
    }
    (Seq.fill(101)(22L -> analytics), 101)
  }

}
