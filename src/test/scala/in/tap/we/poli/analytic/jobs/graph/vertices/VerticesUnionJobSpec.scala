package in.tap.we.poli.analytic.jobs.graph.vertices

import org.scalatest.{FlatSpec, Matchers}

class VerticesUnionJobSpec extends FlatSpec with Matchers with VerticesUnionJobFixtures {

  it should "make vertices agnostic" in {
    import VerticesUnionJob.AgnosticVertex
    AgnosticVertex.fromCommitteeVertex(committeeVertex1) shouldBe {
      AgnosticVertex(
        uid = 11L,
        name = Some("comittee1"),
        is_committee = true
      )
    }

    AgnosticVertex.fromVendorVertex(vendorVertex1) shouldBe {
      AgnosticVertex(
        uid = 1L,
        name = Some("Mickey's Consulting"),
        is_committee = false
      )
    }
  }

}
