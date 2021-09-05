package in.tap.we.poli.analytic.jobs.dynamo.traversal.n2

import in.tap.base.spark.io.{Formats, In, Out}
import in.tap.base.spark.main.InArgs.{OneInArgs, TwoInArgs}
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.BaseSparkJobSpec
import in.tap.we.poli.analytic.jobs.dynamo.traversal.Traversal
import org.apache.spark.sql.Dataset

class GraphTraversalSB1JobSpec extends BaseSparkJobSpec {

  it should "build graph traversal look ups from edges" in {
    val resourcePath: String = {
      getClass.getResource("../../../../../../../../../dynamo/").toString
    }
    val initPath: String = {
      s"$resourcePath/graph_traversal/init/out/"
    }
    val n1Path: String = {
      s"$resourcePath/graph_traversal/n1/in/"
    }
    val inPath: String = {
      s"$resourcePath/graph_traversal/n2/in/"
    }
    val outPath: String = {
      s"$resourcePath/graph_traversal/n2/out/"
    }
    import spark.implicits._
    val _: Unit = {
      new N2InitJob(
        TwoInArgs(In(initPath, Formats.PARQUET), In(n1Path, Formats.PARQUET)),
        OneOutArgs(Out(inPath, Formats.PARQUET))
      ).execute()
      new GraphTraversalSB1Job(
        OneInArgs(In(inPath, Formats.PARQUET)),
        OneOutArgs(Out(outPath, Formats.JSON))
      ).execute()
    }
    val traversals: Dataset[Traversal] = {
      spark
        .read
        .json(outPath)
        .as[Traversal]
    }
    traversals
      .collect()
      .toList
      .sortBy(_.src_ids) shouldBe {
      List(
        )
    }
  }

}
