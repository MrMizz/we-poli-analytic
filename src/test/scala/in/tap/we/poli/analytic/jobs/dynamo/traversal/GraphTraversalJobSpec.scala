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
    // more than two pages
    GraphTraversal.paginate(vertexId = 1L, traversalsWithCount3) shouldBe {
      Seq(
        GraphTraversal(
          vertex_id = 1L,
          page_num = 1L,
          related_vertex_ids = (0 to 99 by 1).map(_.toLong)
        ),
        GraphTraversal(
          vertex_id = 1L,
          page_num = 2L,
          related_vertex_ids = (100 to 199 by 1).map(_.toLong)
        ),
        GraphTraversal(
          vertex_id = 1L,
          page_num = 3L,
          related_vertex_ids = (200 to 299 by 1).map(_.toLong)
        )
      )
    }
  }

}
