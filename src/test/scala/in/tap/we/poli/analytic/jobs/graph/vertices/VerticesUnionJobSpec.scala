package in.tap.we.poli.analytic.jobs.graph.vertices

import org.scalatest.{FlatSpec, Matchers}

class VerticesUnionJobSpec extends FlatSpec with Matchers with VerticesUnionJobFixtures {

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
