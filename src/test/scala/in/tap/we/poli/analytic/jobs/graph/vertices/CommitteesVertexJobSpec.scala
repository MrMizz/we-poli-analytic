package in.tap.we.poli.analytic.jobs.graph.vertices

import org.scalatest.{FlatSpec, Matchers}

class CommitteesVertexJobSpec extends FlatSpec with Matchers with CommitteesVertexFixtures {

  it should "build vertices from committees" in {
    import CommitteesVertexJob.CommitteeVertex
    CommitteeVertex.fromCommittee(committee1) shouldBe {
      "1" -> committeeVertex1
    }

    CommitteeVertex.fromCommittee(committee2) shouldBe {
      "1" -> committeeVertex2
    }
  }

  it should "reduce committee vertices" in {
    import CommitteesVertexJob.CommitteeVertex
    CommitteeVertex.reduce(committeeVertex1, committeeVertex2) shouldBe {
      committeeVertex1
    }

    CommitteeVertex.reduce(committeeVertex1, committeeVertex3) shouldBe {
      committeeVertex4
    }
  }

}
