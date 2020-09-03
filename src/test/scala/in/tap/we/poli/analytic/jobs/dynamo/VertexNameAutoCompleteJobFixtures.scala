package in.tap.we.poli.analytic.jobs.dynamo

import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.{
  AggregateExpenditureEdge, Analytics, ExpenditureEdge
}
import in.tap.we.poli.analytic.jobs.graph.vertices.VerticesUnionJob.AgnosticVertex
import in.tap.we.poli.analytic.jobs.graph.vertices.VerticesUnionJobFixtures

trait VertexNameAutoCompleteJobFixtures extends VerticesUnionJobFixtures {

  val agnosticVertex2: AgnosticVertex = {
    agnosticVertex1.copy(uid = 22L)
  }

  val agnosticVertex3: AgnosticVertex = {
    agnosticVertex1.copy(uid = 33L)
  }

  val agnosticVertex4: AgnosticVertex = {
    agnosticVertex1.copy(uid = 44L)
  }

  val agnosticVertex5: AgnosticVertex = {
    agnosticVertex1.copy(uid = 55L)
  }

  val agnosticVertex6: AgnosticVertex = {
    agnosticVertex1.copy(uid = 66L)
  }

  val agnosticVertex7: AgnosticVertex = {
    agnosticVertex1.copy(uid = 77L)
  }

  val agnosticVertex8: AgnosticVertex = {
    agnosticVertex1.copy(uid = 88L)
  }

  val agnosticVertex9: AgnosticVertex = {
    agnosticVertex1.copy(uid = 99L)
  }

  val agnosticVertex10: AgnosticVertex = {
    agnosticVertex1.copy(uid = 110L)
  }

  val agnosticVertex11: AgnosticVertex = {
    agnosticVertex1.copy(uid = 121L)
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
      analytics = Analytics(
        num_edges = 10,
        total_spend = None,
        avg_spend = None,
        min_spend = None,
        max_spend = None
      ),
      edges = Seq.empty[ExpenditureEdge]
    )
  }

  val aggregateExpenditureEdge2: AggregateExpenditureEdge = {
    aggregateExpenditureEdge1.copy(dst_id = 33)
  }

  val aggregateExpenditureEdge3: AggregateExpenditureEdge = {
    aggregateExpenditureEdge1.copy(dst_id = 44L)
  }

  val aggregateExpenditureEdge4: AggregateExpenditureEdge = {
    aggregateExpenditureEdge1.copy(dst_id = 55L)
  }

  val aggregateExpenditureEdge5: AggregateExpenditureEdge = {
    aggregateExpenditureEdge1.copy(dst_id = 66L)
  }

  val aggregateExpenditureEdge6: AggregateExpenditureEdge = {
    aggregateExpenditureEdge1.copy(dst_id = 77L)
  }

  val aggregateExpenditureEdge7: AggregateExpenditureEdge = {
    aggregateExpenditureEdge1.copy(dst_id = 88L)
  }

  val aggregateExpenditureEdge8: AggregateExpenditureEdge = {
    aggregateExpenditureEdge1.copy(dst_id = 99L)
  }

  val aggregateExpenditureEdge9: AggregateExpenditureEdge = {
    aggregateExpenditureEdge1.copy(dst_id = 110L)
  }

  val aggregateExpenditureEdge10: AggregateExpenditureEdge = {
    aggregateExpenditureEdge1.copy(dst_id = 12L, analytics = aggregateExpenditureEdge1.analytics.copy(num_edges = 9))
  }

  val aggregateExpenditureEdge11: AggregateExpenditureEdge = {
    AggregateExpenditureEdge(
      src_id = 1L,
      dst_id = 2L,
      analytics = Analytics(
        num_edges = 1,
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

}
