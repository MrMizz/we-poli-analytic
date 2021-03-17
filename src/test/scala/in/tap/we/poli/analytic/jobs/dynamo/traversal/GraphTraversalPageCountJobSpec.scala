package in.tap.we.poli.analytic.jobs.dynamo.traversal

import in.tap.base.spark.io.{Formats, In, Out}
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.BaseSparkJobSpec
import in.tap.we.poli.analytic.jobs.dynamo.traversal.GraphTraversalPageCountJob.GraphTraversalPageCount
import org.apache.spark.sql.Dataset
import org.scalatest.DoNotDiscover

@DoNotDiscover
class GraphTraversalPageCountJobSpec extends BaseSparkJobSpec {

  it should "build page counts from traversals" in {
    val resourcePath: String = {
      getClass.getResource("../../../../../../../../dynamo/graph_traversal_page_count/").toString
    }
    val inPath: String = {
      s"$resourcePath/in/"
    }
    val outPath: String = {
      s"$resourcePath/out/"
    }
    import spark.implicits._
    val _: Unit = {
      new GraphTraversalPageCountJob(
        OneInArgs(In(inPath, Formats.PARQUET)),
        OneOutArgs(Out(outPath, Formats.PARQUET))
      ).execute()
    }
    val pageCounts: Dataset[GraphTraversalPageCount] = {
      spark
        .read
        .parquet(outPath)
        .as[GraphTraversalPageCount]
    }
    pageCounts
      .collect
      .toSeq
      .sortBy(_.vertex_id) shouldBe {
      Seq(
        GraphTraversalPageCount(
          vertex_id = 1L,
          page_count = 1L
        ),
        GraphTraversalPageCount(
          vertex_id = 2L,
          page_count = 1L
        ),
        GraphTraversalPageCount(
          vertex_id = 11L,
          page_count = 1L
        ),
        GraphTraversalPageCount(
          vertex_id = 22L,
          page_count = 1L
        ),
        GraphTraversalPageCount(
          vertex_id = 33L,
          page_count = 1L
        ),
        GraphTraversalPageCount(
          vertex_id = 44L,
          page_count = 1L
        ),
        GraphTraversalPageCount(
          vertex_id = 55L,
          page_count = 1L
        ),
        GraphTraversalPageCount(
          vertex_id = 66L,
          page_count = 1L
        ),
        GraphTraversalPageCount(
          vertex_id = 77L,
          page_count = 1L
        ),
        GraphTraversalPageCount(
          vertex_id = 88L,
          page_count = 1L
        ),
        GraphTraversalPageCount(
          vertex_id = 99L,
          page_count = 1L
        ),
        GraphTraversalPageCount(
          vertex_id = 110L,
          page_count = 1L
        ),
        GraphTraversalPageCount(
          vertex_id = 121L,
          page_count = 1L
        )
      )
    }
  }

}
