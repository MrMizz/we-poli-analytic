package in.tap.we.poli.analytic.jobs.dynamo.autocomplete

import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.{
  AggregateExpenditureEdge, Analytics, ExpenditureEdge
}
import in.tap.we.poli.analytic.jobs.graph.vertices.VerticesUnionJob.AgnosticVertex
import in.tap.we.poli.analytic.jobs.graph.vertices.VerticesUnionJobFixtures

trait VertexNameAutoCompleteJobFixtures extends VerticesUnionJobFixtures {

  val agnosticVertex2: AgnosticVertex = {
    agnosticVertex1.copy(uid = 22L, is_committee = false)
  }

  val agnosticVertex3: AgnosticVertex = {
    agnosticVertex2.copy(uid = 33L)
  }

  val agnosticVertex4: AgnosticVertex = {
    agnosticVertex2.copy(uid = 44L)
  }

  val agnosticVertex5: AgnosticVertex = {
    agnosticVertex2.copy(uid = 55L)
  }

  val agnosticVertex6: AgnosticVertex = {
    agnosticVertex2.copy(uid = 66L)
  }

  val agnosticVertex7: AgnosticVertex = {
    agnosticVertex2.copy(uid = 77L)
  }

  val agnosticVertex8: AgnosticVertex = {
    agnosticVertex2.copy(uid = 88L)
  }

  val agnosticVertex9: AgnosticVertex = {
    agnosticVertex2.copy(uid = 99L)
  }

  val agnosticVertex10: AgnosticVertex = {
    agnosticVertex2.copy(uid = 110L)
  }

  val agnosticVertex11: AgnosticVertex = {
    agnosticVertex2.copy(uid = 121L)
  }

  val agnosticVertices: Seq[AgnosticVertex] = {
    Seq(
      agnosticVertex1,
      agnosticVertex2,
      agnosticVertex3,
      agnosticVertex4,
      agnosticVertex5,
      agnosticVertex6,
      agnosticVertex7,
      agnosticVertex8,
      agnosticVertex9,
      agnosticVertex10,
      agnosticVertex11,
      agnosticVertex12
    )
  }

  val aggregateExpenditureEdge1: AggregateExpenditureEdge = {
    AggregateExpenditureEdge(
      src_id = 11L,
      dst_id = 22L,
      analytics = analytics(10),
      edges = Seq.empty[ExpenditureEdge]
    )
  }

  val aggregateExpenditureEdge2: AggregateExpenditureEdge = {
    aggregateExpenditureEdge1.copy(dst_id = 33, analytics = analytics(11))
  }

  val aggregateExpenditureEdge3: AggregateExpenditureEdge = {
    aggregateExpenditureEdge1.copy(dst_id = 44L, analytics = analytics(12))
  }

  val aggregateExpenditureEdge4: AggregateExpenditureEdge = {
    aggregateExpenditureEdge1.copy(dst_id = 55L, analytics = analytics(13))
  }

  val aggregateExpenditureEdge5: AggregateExpenditureEdge = {
    aggregateExpenditureEdge1.copy(dst_id = 66L, analytics = analytics(1))
  }

  val aggregateExpenditureEdge6: AggregateExpenditureEdge = {
    aggregateExpenditureEdge1.copy(dst_id = 77L, analytics = analytics(2))
  }

  val aggregateExpenditureEdge7: AggregateExpenditureEdge = {
    aggregateExpenditureEdge1.copy(dst_id = 88L, analytics = analytics(3))
  }

  val aggregateExpenditureEdge8: AggregateExpenditureEdge = {
    aggregateExpenditureEdge1.copy(dst_id = 99L, analytics = analytics(22))
  }

  val aggregateExpenditureEdge9: AggregateExpenditureEdge = {
    aggregateExpenditureEdge1.copy(dst_id = 110L, analytics = analytics(23))
  }

  val aggregateExpenditureEdge10: AggregateExpenditureEdge = {
    aggregateExpenditureEdge1.copy(dst_id = 121L, analytics = analytics(99))
  }

  val aggregateExpenditureEdge11: AggregateExpenditureEdge = {
    AggregateExpenditureEdge(
      src_id = 1L,
      dst_id = 2L,
      analytics = Analytics(
        num_edges = 101,
        total_spend = None,
        avg_spend = None,
        min_spend = None,
        max_spend = None
      ),
      edges = Seq(ExpenditureEdge(1L, None, None, None, None, None, None, None, None, None, None, None))
    )
  }

  val aggregateExpenditureEdges: Seq[AggregateExpenditureEdge] = {
    Seq(
      aggregateExpenditureEdge1,
      aggregateExpenditureEdge2,
      aggregateExpenditureEdge3,
      aggregateExpenditureEdge4,
      aggregateExpenditureEdge5,
      aggregateExpenditureEdge6,
      aggregateExpenditureEdge7,
      aggregateExpenditureEdge8,
      aggregateExpenditureEdge9,
      aggregateExpenditureEdge10,
      aggregateExpenditureEdge11
    )
  }

  def analytics(numEdges: Long): Analytics = {
    Analytics(
      num_edges = numEdges,
      total_spend = None,
      avg_spend = None,
      min_spend = None,
      max_spend = None
    )
  }

}
