package in.tap.we.poli.analytic.jobs.graph.edges

import in.tap.we.poli.analytic.jobs.BaseSpec

class CommitteeToVendorEdgeJobSpec extends BaseSpec with CommitteeToVendorEdgeJobFixtures {

  it should "build graphx edges from unique vendors" in {
    import CommitteeToVendorEdgeJob.ExpenditureEdge
    ExpenditureEdge.fromUniqueVendor(uniqueVendor1) shouldBe {
      Seq((6L, 1L) -> Seq(edge1))
    }

    ExpenditureEdge.fromUniqueVendor(uniqueVendor2) shouldBe {
      Seq(
        (7L, 2L) -> Seq(edge2),
        (8L, 2L) -> Seq(edge3),
        (8L, 2L) -> Seq(edge4)
      )
    }

    ExpenditureEdge.fromUniqueVendor(uniqueVendor3) shouldBe {
      Seq((10L, 5L) -> Seq(edge5))
    }
  }

}
