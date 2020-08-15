package in.tap.we.poli.analytic.jobs.graph.vertices

import in.tap.we.poli.analytic.jobs.graph.vertices.VerticesUnionJob.AgnosticVertex

trait VerticesUnionJobFixtures extends CommitteesVertexJobFixtures with VendorsVertexJobFixtures {

  val agnosticVertex1: AgnosticVertex = {
    AgnosticVertex(
      uid = 11L,
      name = "committee1",
      streets = Set("street1", "street2"),
      cities = Set("city1"),
      states = Set("state1"),
      is_committee = true
    )
  }

  val agnosticVertex12: AgnosticVertex = {
    AgnosticVertex(
      uid = 1L,
      name = "Mickey's Consulting",
      streets = Set.empty[String],
      cities = Set("Los Angeles"),
      states = Set.empty[String],
      is_committee = false
    )
  }

}
