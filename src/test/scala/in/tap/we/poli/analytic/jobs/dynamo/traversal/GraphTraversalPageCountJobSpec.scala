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
    val _: Unit = {
      new GraphTraversalPageCountJob(
        OneInArgs(In(inPath, Formats.PARQUET)),
        OneOutArgs(Out(outPath, Formats.PARQUET))
      ).execute()
    }
    import spark.implicits._
    val pageCounts: Dataset[GraphTraversalPageCount] = {
      spark
        .read
        .parquet(outPath)
        .as[GraphTraversalPageCount]
    }
    pageCounts
      .collect
      .toSeq
      .sortBy(_.src_ids.toLong) shouldBe {
      Seq(
        GraphTraversalPageCount(
          src_ids = "1",
          page_count = 1L
        ),
        GraphTraversalPageCount(
          src_ids = "2",
          page_count = 1L
        ),
        GraphTraversalPageCount(
          src_ids = "11",
          page_count = 1L
        ),
        GraphTraversalPageCount(
          src_ids = "22",
          page_count = 1L
        ),
        GraphTraversalPageCount(
          src_ids = "33",
          page_count = 1L
        ),
        GraphTraversalPageCount(
          src_ids = "44",
          page_count = 1L
        ),
        GraphTraversalPageCount(
          src_ids = "55",
          page_count = 1L
        ),
        GraphTraversalPageCount(
          src_ids = "66",
          page_count = 1L
        ),
        GraphTraversalPageCount(
          src_ids = "77",
          page_count = 1L
        ),
        GraphTraversalPageCount(
          src_ids = "88",
          page_count = 1L
        ),
        GraphTraversalPageCount(
          src_ids = "99",
          page_count = 1L
        ),
        GraphTraversalPageCount(
          src_ids = "110",
          page_count = 1L
        ),
        GraphTraversalPageCount(
          src_ids = "121",
          page_count = 1L
        )
      )
    }
  }

}
