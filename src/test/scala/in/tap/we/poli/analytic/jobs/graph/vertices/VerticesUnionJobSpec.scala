package in.tap.we.poli.analytic.jobs.graph.vertices

import org.scalatest.{FlatSpec, Matchers}

class VerticesUnionJobSpec extends FlatSpec with Matchers with VerticesUnionJobFixtures {

  it should "make vertices agnostic" in {
    import VerticesUnionJob.AgnosticVertex
    AgnosticVertex.fromCommitteeVertex(committeeVertex1) shouldBe {
      AgnosticVertex(
        uid = 11L,
        is_committee = true,
        is_vendor = false,
        committee_attr = Some(committeeVertex1),
        vendor_attr = None
      )
    }

    AgnosticVertex.fromVendorVertex(vendorVertex1) shouldBe {
      AgnosticVertex(
        uid = 1L,
        is_committee = false,
        is_vendor = true,
        committee_attr = None,
        vendor_attr = Some(vendorVertex1)
      )
    }
  }

}
