package in.tap.we.poli.analytic.jobs.dynamo.traversal.n2

import in.tap.base.spark.io.{Formats, In, Out}
import in.tap.base.spark.main.InArgs.{OneInArgs, TwoInArgs}
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.BaseSparkJobSpec
import in.tap.we.poli.analytic.jobs.dynamo.traversal.Traversal

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
        OneOutArgs(Out(outPath, Formats.PARQUET))
      ).execute()
    }
    val traversals: List[Traversal] = {
      spark
        .read
        .parquet(outPath)
        .as[Traversal]
        .collect()
        .toList
        .sortBy(_.src_ids)
    }
    traversals.foreach { tr =>
      val pp = {
        s"Traversal(${tr.src_ids}, ${tr.page_num}L, ${tr.dst_ids}),"
      }
      println(pp)
    }
    traversals shouldBe {
      List(
        Traversal("110_121", 1L, List(11)),
        Traversal("22_110", 1L, List(11)),
        Traversal("22_121", 1L, List(11)),
        Traversal("22_33", 1L, List(11)),
        Traversal("22_44", 1L, List(11)),
        Traversal("22_55", 1L, List(11)),
        Traversal("22_66", 1L, List(11)),
        Traversal("22_77", 1L, List(11)),
        Traversal("22_88", 1L, List(11)),
        Traversal("22_99", 1L, List(11)),
        Traversal("33_110", 1L, List(11)),
        Traversal("33_121", 1L, List(11)),
        Traversal("33_44", 1L, List(11)),
        Traversal("33_55", 1L, List(11)),
        Traversal("33_66", 1L, List(11)),
        Traversal("33_77", 1L, List(11)),
        Traversal("33_88", 1L, List(11)),
        Traversal("33_99", 1L, List(11)),
        Traversal("44_110", 1L, List(11)),
        Traversal("44_121", 1L, List(11)),
        Traversal("44_55", 1L, List(11)),
        Traversal("44_66", 1L, List(11)),
        Traversal("44_77", 1L, List(11)),
        Traversal("44_88", 1L, List(11)),
        Traversal("44_99", 1L, List(11)),
        Traversal("55_110", 1L, List(11)),
        Traversal("55_121", 1L, List(11)),
        Traversal("55_66", 1L, List(11)),
        Traversal("55_77", 1L, List(11)),
        Traversal("55_88", 1L, List(11)),
        Traversal("55_99", 1L, List(11)),
        Traversal("66_110", 1L, List(11)),
        Traversal("66_121", 1L, List(11)),
        Traversal("66_77", 1L, List(11)),
        Traversal("66_88", 1L, List(11)),
        Traversal("66_99", 1L, List(11)),
        Traversal("77_110", 1L, List(11)),
        Traversal("77_121", 1L, List(11)),
        Traversal("77_88", 1L, List(11)),
        Traversal("77_99", 1L, List(11)),
        Traversal("88_110", 1L, List(11)),
        Traversal("88_121", 1L, List(11)),
        Traversal("88_99", 1L, List(11)),
        Traversal("99_110", 1L, List(11)),
        Traversal("99_121", 1L, List(11))
      )
    }
  }

}
