package in.tap.we.poli.analytic.jobs.graph.vertices

import in.tap.we.poli.analytic.jobs.BaseSpec

class VerticesUnionJobSpec extends BaseSpec with VerticesUnionJobFixtures {

  it should "make vertices agnostic" in {
    import VerticesUnionJob.AgnosticVertex
    AgnosticVertex.fromCommitteeVertex(committeeVertex1) shouldBe {
      agnosticVertex1
    }

    AgnosticVertex.fromVendorVertex(vendorVertex1) shouldBe {
      agnosticVertex12
    }
  }

}
