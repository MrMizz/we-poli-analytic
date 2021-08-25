package in.tap.we.poli.analytic.jobs.dynamo.traversal.n1

import in.tap.we.poli.analytic.jobs.dynamo.traversal.n1.GraphTraversalJob.GraphTraversal.TraversalWithCount
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

  val traversalsWithCount3: TraversalWithCount = {
    val analytics = {
      Analytics(num_edges = 1000, None, None, None, None)
    }
    val seq: Seq[(Long, Analytics)] = {
      (0 to 299 by 1).map { i =>
        i.toLong -> analytics
      }
    }
    (seq, 300L)
  }

}
