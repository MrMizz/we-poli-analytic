package in.tap.we.poli.analytic.jobs.dynamo.traversal.n1

import in.tap.base.spark.io.{Formats, In, Out}
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.BaseSparkJobSpec
import in.tap.we.poli.analytic.jobs.dynamo.autocomplete.VertexNameAutoCompleteJobFixtures
import in.tap.we.poli.analytic.jobs.dynamo.traversal.Traversal
import org.apache.spark.sql.{Dataset, SaveMode}
import org.scalatest.DoNotDiscover

@DoNotDiscover
class GraphTraversalSB1JobSpec extends BaseSparkJobSpec with VertexNameAutoCompleteJobFixtures {

  it should "build graph traversal look ups from edges" in {
    val resourcePath: String = {
      getClass.getResource("../../../../../../../../../dynamo/").toString
    }
    val inPath: String = {
      s"$resourcePath/graph_traversal/n1/in/"
    }
    val outPath: String = {
      s"$resourcePath/graph_traversal/n1/out/"
    }
    val pageCountInPath: String = {
      s"$resourcePath/graph_traversal_page_count/in/"
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
        OneOutArgs(Out(outPath, Formats.PARQUET))
      ).execute()
    }
    val graphTraversals: Dataset[Traversal] = {
      val ds: Dataset[Traversal] = spark
        .read
        .parquet(outPath)
        .as[Traversal]
      val _: Unit = {
        ds.write
          .mode(SaveMode.Overwrite)
          .parquet(pageCountInPath)
      }
      ds
    }
    graphTraversals
      .collect()
      .toSeq
      .sortBy(_.src_ids.toLong)
      .map { graphTraversal: Traversal =>
        (graphTraversal.src_ids.toLong, graphTraversal.page_num, graphTraversal.dst_ids)
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
          Seq(121L, 110L, 99L, 55L, 44L, 33L, 22L, 88L, 77L, 66L)
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
        ),
        (
          121L,
          1L,
          Seq(11L)
        )
      )
    }
  }

}
