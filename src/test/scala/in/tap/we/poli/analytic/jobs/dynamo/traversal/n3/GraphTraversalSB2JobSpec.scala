package in.tap.we.poli.analytic.jobs.dynamo.traversal.n3

import in.tap.base.spark.io.{Formats, In, Out}
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.BaseSparkJobSpec
import in.tap.we.poli.analytic.jobs.dynamo.traversal.Traversal
import in.tap.we.poli.analytic.jobs.dynamo.traversal.n2.Fixtures
import org.apache.spark.sql.{Dataset, SaveMode}

class GraphTraversalSB2JobSpec extends BaseSparkJobSpec with Fixtures {

  it should "build graph traversal look ups from edges" in {
    val resourcePath: String = {
      getClass.getResource("../../../../../../../../../dynamo/").toString
    }
    val inPath: String = {
      s"$resourcePath/graph_traversal/n3/in/"
    }
    val outPath: String = {
      s"$resourcePath/graph_traversal/n3/out/"
    }
    import spark.implicits._
    val _: Unit = {
      edges
        .toDS()
        .write
        .mode(SaveMode.Overwrite)
        .parquet(inPath)
      new GraphTraversalSB2Job(
        OneInArgs(In(inPath, Formats.PARQUET)),
        OneOutArgs(Out(outPath, Formats.PARQUET))
      ).execute()
    }
    val traversals: Dataset[Traversal] = {
      spark
        .read
        .parquet(outPath)
        .as[Traversal]
    }
    traversals
      .collect()
      .toList
      .sortBy(_.src_ids) shouldBe {
      List(
        Traversal(
          src_ids = "5_6_7",
          page_num = 1L,
          dst_ids = Seq(4L)
        )
      )
    }
  }

}