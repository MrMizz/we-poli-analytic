package in.tap.we.poli.analytic.jobs.dynamo.traversal.n2

import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.{
  AggregateExpenditureEdge, Analytics, ExpenditureEdge
}

trait Fixtures {

  val aggregateExpenditureEdge1: AggregateExpenditureEdge = {
    edge(
      srcId = 1L,
      dstId = 2L,
      totalSpend = Some(1L)
    )
  }

  val aggregateExpenditureEdge2: AggregateExpenditureEdge = {
    edge(
      srcId = 1L,
      dstId = 3L,
      totalSpend = Some(2L)
    )
  }

  val aggregateExpenditureEdge3: AggregateExpenditureEdge = {
    edge(
      srcId = 4L,
      dstId = 5L,
      totalSpend = Some(3L)
    )
  }

  val aggregateExpenditureEdge4: AggregateExpenditureEdge = {
    edge(
      srcId = 4L,
      dstId = 6L,
      totalSpend = None
    )
  }

  val aggregateExpenditureEdge5: AggregateExpenditureEdge = {
    edge(
      srcId = 4L,
      dstId = 7L,
      totalSpend = Some(2L)
    )
  }

  val aggregateExpenditureEdge6: AggregateExpenditureEdge = {
    edge(
      srcId = 8L,
      dstId = 5L,
      totalSpend = Some(4L)
    )
  }

  val aggregateExpenditureEdge7: AggregateExpenditureEdge = {
    edge(
      srcId = 8L,
      dstId = 6L,
      totalSpend = Some(2L)
    )
  }


  val aggregateExpenditureEdge8: AggregateExpenditureEdge = {
    edge(
      srcId = 9L,
      dstId = 10L,
      totalSpend = Some(5L)
    )
  }

  val edges: Seq[AggregateExpenditureEdge] = {
    Seq(
      aggregateExpenditureEdge1,
      aggregateExpenditureEdge2,
      aggregateExpenditureEdge3,
      aggregateExpenditureEdge4,
      aggregateExpenditureEdge5,
      aggregateExpenditureEdge6,
      aggregateExpenditureEdge7,
      aggregateExpenditureEdge8
    )
  }

  private def analytics(maybeDouble: Option[Double]): Analytics = {
    Analytics(
      num_edges = 1L,
      total_spend = maybeDouble,
      avg_spend = None,
      min_spend = None,
      max_spend = None
    )
  }

  private def edge(srcId: Long, dstId: Long, totalSpend: Option[Double]): AggregateExpenditureEdge = {
    AggregateExpenditureEdge(
      src_id = srcId,
      dst_id = dstId,
      analytics = analytics(totalSpend),
      edges = Seq.empty[ExpenditureEdge]
    )
  }

}
