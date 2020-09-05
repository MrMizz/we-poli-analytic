package in.tap.we.poli.analytic.jobs.dynamo

import in.tap.base.spark.io.{Formats, In, Out}
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.TwoOutArgs
import in.tap.we.poli.analytic.jobs.BaseSparkJobSpec
import in.tap.we.poli.analytic.jobs.dynamo.GraphTraversalJob.GraphTraversal
import org.apache.spark.sql.SaveMode

class GraphTraversalJobSpec extends BaseSparkJobSpec with VertexNameAutoCompleteJobFixtures {

  it should "paginate" in {
    // one page
    GraphTraversal.paginate(vertexId = 10L, relatedVertexIdsWithCount = (Seq.fill(99)(2L), 99)) shouldBe {
      Seq(
        GraphTraversal(
          vertex_id = 10L,
          page_num = 1L,
          related_vertex_ids = Seq.fill(99)(2L)
        ) -> (10L, 1L)
      )
    }
    // two pages
    GraphTraversal.paginate(vertexId = 10L, relatedVertexIdsWithCount = (Seq.fill(101)(2L), 101)) shouldBe {
      Seq(
        GraphTraversal(
          vertex_id = 10L,
          page_num = 1L,
          related_vertex_ids = Seq.fill(100)(2L)
        ) -> (10L, 1L),
        GraphTraversal(
          vertex_id = 10L,
          page_num = 2L,
          related_vertex_ids = Seq(2L)
        ) -> (10L, 1L)
      )
    }
  }

  it should "build graph traversal look ups from edges" in {
    val resourcePath: String = {
      "/Users/alex/Documents/GitHub/Alex/tap-in/we-poli/we-poli-analytic/src/test/resources/dynamo/graph_traversal"
    }
    val inPath: String = {
      s"$resourcePath/in/"
    }
    val outPath1: String = {
      s"$resourcePath/out1/"
    }
    val outPath2: String = {
      s"$resourcePath/out2/"
    }
    import spark.implicits._
    val _: Unit = {
      aggregateExpenditureEdges.toDS().write.mode(SaveMode.Overwrite).parquet(inPath)
      new GraphTraversalJob(
        OneInArgs(In(inPath, Formats.PARQUET)),
        TwoOutArgs(Out(outPath1, Formats.PARQUET), Out(outPath2, Formats.PARQUET))
      ).execute()
    }
    spark.read.parquet(outPath1).as[GraphTraversal].collect().toSeq.sortBy(_.vertex_id).map {
      graphTraversal: GraphTraversal =>
        (graphTraversal.vertex_id, graphTraversal.page_num, graphTraversal.related_vertex_ids.sorted)
    } shouldBe {
      Seq(
        (
          1L,
          1L,
          Seq(2L)
        ),
        (
          2L,
          1L,
          Seq(1L)
        ),
        (
          11L,
          1L,
          Seq(12L, 22L, 33L, 44L, 55L, 66L, 77L, 88L, 99L, 110L)
        ),
        (
          12L,
          1L,
          Seq(11L)
        ),
        (
          22L,
          1L,
          Seq(11L)
        ),
        (
          33L,
          1L,
          Seq(11L)
        ),
        (
          44L,
          1L,
          Seq(11L)
        ),
        (
          55L,
          1L,
          Seq(11L)
        ),
        (
          66L,
          1L,
          Seq(11L)
        ),
        (
          77L,
          1L,
          Seq(11L)
        ),
        (
          88L,
          1L,
          Seq(11L)
        ),
        (
          99L,
          1L,
          Seq(11L)
        ),
        (
          110L,
          1L,
          Seq(11L)
        )
      )
    }
  }

}
