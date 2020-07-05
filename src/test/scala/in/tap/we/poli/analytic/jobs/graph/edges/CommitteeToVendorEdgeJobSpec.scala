package in.tap.we.poli.analytic.jobs.graph.edges

import org.apache.spark.graphx.Edge
import org.scalatest.{FlatSpec, Matchers}

class CommitteeToVendorEdgeJobSpec extends FlatSpec with Matchers with CommitteeToVendorEdgeJobFixtures {

  it should "build graphx edges from unique vendors" in {
    import CommitteeToVendorEdgeJob.ExpenditureEdge
    ExpenditureEdge.fromUniqueVendor(uniqueVendor1) shouldBe {
      Seq(
        Edge(
          srcId = 6L,
          dstId = 1L,
          attr = edge1
        )
      )
    }

    ExpenditureEdge.fromUniqueVendor(uniqueVendor2) shouldBe {
      Seq(
        Edge(
          srcId = 7L,
          dstId = 2L,
          attr = edge2
        ),
        Edge(
          srcId = 8L,
          dstId = 2L,
          attr = edge3
        ),
        Edge(
          srcId = 8L,
          dstId = 2L,
          attr = edge4
        )
      )
    }

    ExpenditureEdge.fromUniqueVendor(uniqueVendor3) shouldBe {
      Seq(
        Edge(
          srcId = 10L,
          dstId = 5L,
          attr = edge5
        )
      )
    }
  }

}
