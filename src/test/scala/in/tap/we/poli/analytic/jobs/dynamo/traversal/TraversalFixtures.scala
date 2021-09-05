package in.tap.we.poli.analytic.jobs.dynamo.traversal

import in.tap.we.poli.analytic.jobs.dynamo.traversal.nx.InitJob.DstId
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.Analytics

trait TraversalFixtures {

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

  val dstId1: DstId = {
    DstId(1L, emptyAnalytics.copy(num_edges = 1L))
  }

  val dstId2: DstId = {
    DstId(2L, emptyAnalytics.copy(num_edges = 5L))
  }

  val dstId3: DstId = {
    DstId(3L, emptyAnalytics.copy(num_edges = 3L))
  }

  val dstId4: DstId = {
    DstId(4L, emptyAnalytics.copy(max_spend = None))
  }

  val dstId5: DstId = {
    DstId(5L, emptyAnalytics.copy(max_spend = Some(50d)))
  }

  val dstId6: DstId = {
    DstId(6L, emptyAnalytics.copy(max_spend = Some(40d)))
  }

  val dstId7: DstId = {
    DstId(7L, emptyAnalytics.copy(max_spend = Some(70d)))
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
