package in.tap.we.poli.analytic.jobs.dynamo

import in.tap.base.spark.io.{Formats, In, Out}
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.BaseSparkJobSpec
import in.tap.we.poli.analytic.jobs.dynamo.GraphTraversalJob.GraphTraversal
import org.apache.spark.sql.SaveMode

class GraphTraversalJobSpec extends BaseSparkJobSpec with VertexNameAutoCompleteJobFixtures {

  it should "build graph traversal look ups from edges" in {
    val resourcePath: String = {
      "/Users/alex/Documents/GitHub/Alex/tap-in/we-poli/we-poli-analytic/src/test/resources/dynamo/graph_traversal"
    }
    val inPath: String = {
      s"$resourcePath/in/"
    }
    val outPath: String = {
      s"$resourcePath/out/"
    }
    import spark.implicits._
    val _: Unit = {
      aggregateExpenditureEdges.toDS().write.mode(SaveMode.Overwrite).parquet(inPath)
      new GraphTraversalJob(
        OneInArgs(In(inPath, Formats.PARQUET)),
        OneOutArgs(Out(outPath, Formats.PARQUET))
      ).execute()
    }
    import GraphTraversal.Directions.{IN, OUT}
    spark.read.parquet(outPath).as[GraphTraversal].collect().toSeq.sortBy(_.vertex_id).map {
      graphTraversal: GraphTraversal =>
        (graphTraversal.vertex_id, graphTraversal.direction, graphTraversal.related_vertex_ids.sorted)
    } shouldBe {
      Seq(
        (
          1L,
          OUT,
          Seq(2L)
        ),
        (
          2L,
          IN,
          Seq(1L)
        ),
        (
          11L,
          OUT,
          Seq(12L, 22L, 33L, 44L, 55L, 66L, 77L, 88L, 99L, 110L)
        ),
        (
          12L,
          IN,
          Seq(11L)
        ),
        (
          22L,
          IN,
          Seq(11L)
        ),
        (
          33L,
          IN,
          Seq(11L)
        ),
        (
          44L,
          IN,
          Seq(11L)
        ),
        (
          55L,
          IN,
          Seq(11L)
        ),
        (
          66L,
          IN,
          Seq(11L)
        ),
        (
          77L,
          IN,
          Seq(11L)
        ),
        (
          88L,
          IN,
          Seq(11L)
        ),
        (
          99L,
          IN,
          Seq(11L)
        ),
        (
          110L,
          IN,
          Seq(11L)
        )
      )
    }
  }

}
