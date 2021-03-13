package in.tap.we.poli.analytic.jobs.dynamo.traversal

import in.tap.base.spark.io.{Formats, In, Out}
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.TwoOutArgs
import in.tap.we.poli.analytic.jobs.BaseSparkJobSpec
import in.tap.we.poli.analytic.jobs.dynamo.autocomplete.VertexNameAutoCompleteJobFixtures
import in.tap.we.poli.analytic.jobs.dynamo.traversal.GraphTraversalJob.GraphTraversal
import org.apache.spark.sql.SaveMode

class GraphTraversalSB1JobSpec extends BaseSparkJobSpec with VertexNameAutoCompleteJobFixtures {

  it should "build graph traversal look ups from edges" in {
    val resourcePath: String = {
      getClass.getResource("../../../../../../../../dynamo/graph_traversal/").toString
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
      aggregateExpenditureEdges
        .toDS()
        .write
        .mode(SaveMode.Overwrite)
        .parquet(inPath)
      new GraphTraversalSB1Job(
        OneInArgs(In(inPath, Formats.PARQUET)),
        TwoOutArgs(Out(outPath1, Formats.PARQUET), Out(outPath2, Formats.PARQUET))
      ).execute()
    }
    spark
      .read
      .parquet(outPath1)
      .as[GraphTraversal]
      .collect()
      .toSeq
      .sortBy(_.vertex_id)
      .map { graphTraversal: GraphTraversal =>
        (graphTraversal.vertex_id, graphTraversal.page_num, graphTraversal.related_vertex_ids)
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
          Seq(12L, 110L, 99L, 55L, 44L, 33L, 22L, 88L, 77L, 66L)
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
