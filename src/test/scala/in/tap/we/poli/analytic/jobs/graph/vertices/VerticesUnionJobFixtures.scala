package in.tap.we.poli.analytic.jobs.graph.vertices

import in.tap.we.poli.analytic.jobs.graph.vertices.VerticesUnionJob.AgnosticVertex
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.Address

trait VerticesUnionJobFixtures extends CommitteesVertexJobFixtures with VendorsVertexJobFixtures {

  val agnosticVertex1: AgnosticVertex = {
    AgnosticVertex(
      uid = 11L,
      name = "committee1",
      alternate_names = Set.empty[String],
      address = Address(
        street = Option("street1"),
        alternate_street = Option("street2"),
        city = Option("city1"),
        state = Option("state1"),
        zip_code = Some("zip1")
      ),
      alternate_addresses = Set.empty[Address],
      is_committee = true
    )
  }

  val agnosticVertex12: AgnosticVertex = {
    AgnosticVertex(
      uid = 1L,
      name = "Mickey's Consulting",
      alternate_names = Set.empty[String],
      address = Address
        .empty
        .copy(
          city = Some("Los Angeles")
        ),
      alternate_addresses = Set.empty[Address],
      is_committee = false
    )
  }

}
