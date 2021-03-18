package in.tap.we.poli.analytic.jobs.dynamo.traversal

import in.tap.we.poli.analytic.jobs.BaseSpec
import in.tap.we.poli.analytic.jobs.dynamo.traversal.GraphTraversalJob.GraphTraversal

class GraphTraversalJobSpec extends BaseSpec with GraphTraversalJobFixtures {

  it should "paginate" in {
    // one page
    GraphTraversal.paginate(vertexId = 10L, traversalWithCount1) shouldBe {
      Seq(
        GraphTraversal(
          vertex_id = 10L,
          page_num = 1L,
          related_vertex_ids = Seq.fill(99)(22L)
        )
      )
    }
    // two pages
    GraphTraversal.paginate(vertexId = 10L, traversalWithCount2) shouldBe {
      Seq(
        GraphTraversal(
          vertex_id = 10L,
          page_num = 1L,
          related_vertex_ids = Seq.fill(100)(22L)
        ),
        GraphTraversal(
          vertex_id = 10L,
          page_num = 2L,
          related_vertex_ids = Seq(22L)
        )
      )
    }
  }

}
