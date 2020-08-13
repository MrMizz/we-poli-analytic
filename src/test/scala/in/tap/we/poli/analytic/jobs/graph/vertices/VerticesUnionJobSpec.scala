package in.tap.we.poli.analytic.jobs.graph.vertices

import org.scalatest.{FlatSpec, Matchers}

class VerticesUnionJobSpec extends FlatSpec with Matchers with VerticesUnionJobFixtures {

  it should "make vertices agnostic" in {
    import VerticesUnionJob.AgnosticVertex
    AgnosticVertex.fromCommitteeVertex(committeeVertex1) shouldBe {
      AgnosticVertex(
        uid = 11L,
        name = "committee1",
        streets = Set("street1", "street2"),
        cities = Set("city1"),
        states = Set("state1"),
        is_committee = true
      )
    }

    AgnosticVertex.fromVendorVertex(vendorVertex1) shouldBe {
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

}
