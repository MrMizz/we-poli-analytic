package in.tap.we.poli.analytic.jobs.dynamo.traversal

import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.Analytics

trait GraphTraversalJobFixtures {

  val traversal1: Seq[Long] = {
    Seq.fill(99)(22L)
  }

  val traversal2: Seq[Long] = {
    Seq.fill(101)(22L)
  }

  val traversal3: Seq[Long] = {
    val seq: Seq[Long] = {
      (0 to 299 by 1).map { i =>
        i.toLong
      }
    }
    seq
  }

  val traversalWithAnalytics1: (Long, Analytics) = {
    1L -> emptyAnalytics.copy(num_edges = 1L)
  }

  val traversalWithAnalytics2: (Long, Analytics) = {
    2L -> emptyAnalytics.copy(num_edges = 5L)
  }

  val traversalWithAnalytics3: (Long, Analytics) = {
    3L -> emptyAnalytics.copy(num_edges = 3L)
  }

  val traversalWithAnalytics4: (Long, Analytics) = {
    4L -> emptyAnalytics.copy(max_spend = None)
  }

  val traversalWithAnalytics5: (Long, Analytics) = {
    5L -> emptyAnalytics.copy(max_spend = Some(50D))
  }

  val traversalWithAnalytics6: (Long, Analytics) = {
    6L -> emptyAnalytics.copy(max_spend = Some(40D))
  }

  val traversalWithAnalytics7: (Long, Analytics) = {
    7L -> emptyAnalytics.copy(max_spend = Some(70D))
  }

  lazy val emptyAnalytics: Analytics = {
    Analytics(
      num_edges = 1L,
      total_spend = None,
      avg_spend = None,
      min_spend = None,
      max_spend = None
    )
  }

}
