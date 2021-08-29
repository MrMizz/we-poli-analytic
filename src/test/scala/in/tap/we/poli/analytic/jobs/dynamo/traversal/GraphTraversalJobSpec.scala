package in.tap.we.poli.analytic.jobs.dynamo.traversal

import in.tap.we.poli.analytic.jobs.BaseSpec
import in.tap.we.poli.analytic.jobs.dynamo.traversal.n1.GraphTraversalSB1Job

class GraphTraversalJobSpec extends BaseSpec with GraphTraversalJobFixtures {

  it should "paginate" in {
    // one page
    Traversal.paginate(srcIds = "10L", traversal1, 99L) shouldBe {
      Seq(
        Traversal(
          src_ids = "10L",
          page_num = 1L,
          dst_ids = Seq.fill(99)(22L)
        )
      )
    }
    // two pages
    Traversal.paginate(srcIds = "10L", traversal2, 101L) shouldBe {
      Seq(
        Traversal(
          src_ids = "10L",
          page_num = 1L,
          dst_ids = Seq.fill(100)(22L)
        ),
        Traversal(
          src_ids = "10L",
          page_num = 2L,
          dst_ids = Seq(22L)
        )
      )
    }
    // more than two pages
    Traversal.paginate(srcIds = "1L", traversal3, count = 300L) shouldBe {
      Seq(
        Traversal(
          src_ids = "1L",
          page_num = 1L,
          dst_ids = (0 to 99 by 1).map(_.toLong)
        ),
        Traversal(
          src_ids = "1L",
          page_num = 2L,
          dst_ids = (100 to 199 by 1).map(_.toLong)
        ),
        Traversal(
          src_ids = "1L",
          page_num = 3L,
          dst_ids = (200 to 299 by 1).map(_.toLong)
        )
      )
    }
  }

  it should "sort" in {
    // num-edges
    Traversal.sort(
      GraphTraversalSB1Job.sortBy,
      Seq(
        traversalWithAnalytics1,
        traversalWithAnalytics2,
        traversalWithAnalytics3
      )
    ) shouldBe {
      Seq(
        2L,
        3L,
        1L
      )
    }
    // max-spend
    Traversal.sort(
      _.max_spend,
      Seq(
        traversalWithAnalytics4,
        traversalWithAnalytics5,
        traversalWithAnalytics6,
        traversalWithAnalytics7
      )
    ) shouldBe {
      Seq(
        7,
        5,
        6,
        4
      )
    }
  }

}
