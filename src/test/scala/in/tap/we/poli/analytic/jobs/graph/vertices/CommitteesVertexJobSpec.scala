package in.tap.we.poli.analytic.jobs.graph.vertices

import in.tap.we.poli.analytic.jobs.BaseSpec

class CommitteesVertexJobSpec extends BaseSpec with CommitteesVertexJobFixtures {

  it should "build vertices from committees" in {
    import CommitteesVertexJob.CommitteeVertex
    CommitteeVertex.fromCommittee(committee1) shouldBe {
      Some(11L -> committeeVertex1)
    }

    CommitteeVertex.fromCommittee(committee2) shouldBe {
      None
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
