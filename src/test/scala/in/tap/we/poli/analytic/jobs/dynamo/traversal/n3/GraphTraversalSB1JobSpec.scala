package in.tap.we.poli.analytic.jobs.dynamo.traversal.n3

import in.tap.base.spark.io.{Formats, In, Out}
import in.tap.base.spark.main.InArgs.{OneInArgs, TwoInArgs}
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.BaseSparkJobSpec
import in.tap.we.poli.analytic.jobs.dynamo.traversal.Traversal
import org.scalatest.DoNotDiscover

@DoNotDiscover
class GraphTraversalSB1JobSpec extends BaseSparkJobSpec {

  it should "build graph traversal look ups from edges" in {
    val resourcePath: String = {
      getClass.getResource("../../../../../../../../../dynamo/").toString
    }
    val initPath: String = {
      s"$resourcePath/graph_traversal/init/out/"
    }
    val n2Path: String = {
      s"$resourcePath/graph_traversal/n2/in/"
    }
    val inPath: String = {
      s"$resourcePath/graph_traversal/n3/in/"
    }
    val outPath: String = {
      s"$resourcePath/graph_traversal/n3/out/"
    }
    import spark.implicits._
    val _: Unit = {
      new N3InitJob(
        TwoInArgs(In(initPath, Formats.PARQUET), In(n2Path, Formats.PARQUET)),
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
        Traversal("22_110_121", 1L, List(11)),
        Traversal("22_33_110", 1L, List(11)),
        Traversal("22_33_121", 1L, List(11)),
        Traversal("22_33_44", 1L, List(11)),
        Traversal("22_33_55", 1L, List(11)),
        Traversal("22_33_66", 1L, List(11)),
        Traversal("22_33_77", 1L, List(11)),
        Traversal("22_33_88", 1L, List(11)),
        Traversal("22_33_99", 1L, List(11)),
        Traversal("22_44_110", 1L, List(11)),
        Traversal("22_44_121", 1L, List(11)),
        Traversal("22_44_55", 1L, List(11)),
        Traversal("22_44_66", 1L, List(11)),
        Traversal("22_44_77", 1L, List(11)),
        Traversal("22_44_88", 1L, List(11)),
        Traversal("22_44_99", 1L, List(11)),
        Traversal("22_55_110", 1L, List(11)),
        Traversal("22_55_121", 1L, List(11)),
        Traversal("22_55_66", 1L, List(11)),
        Traversal("22_55_77", 1L, List(11)),
        Traversal("22_55_88", 1L, List(11)),
        Traversal("22_55_99", 1L, List(11)),
        Traversal("22_66_110", 1L, List(11)),
        Traversal("22_66_121", 1L, List(11)),
        Traversal("22_66_77", 1L, List(11)),
        Traversal("22_66_88", 1L, List(11)),
        Traversal("22_66_99", 1L, List(11)),
        Traversal("22_77_110", 1L, List(11)),
        Traversal("22_77_121", 1L, List(11)),
        Traversal("22_77_88", 1L, List(11)),
        Traversal("22_77_99", 1L, List(11)),
        Traversal("22_88_110", 1L, List(11)),
        Traversal("22_88_121", 1L, List(11)),
        Traversal("22_88_99", 1L, List(11)),
        Traversal("22_99_110", 1L, List(11)),
        Traversal("22_99_121", 1L, List(11)),
        Traversal("33_110_121", 1L, List(11)),
        Traversal("33_44_110", 1L, List(11)),
        Traversal("33_44_121", 1L, List(11)),
        Traversal("33_44_55", 1L, List(11)),
        Traversal("33_44_66", 1L, List(11)),
        Traversal("33_44_77", 1L, List(11)),
        Traversal("33_44_88", 1L, List(11)),
        Traversal("33_44_99", 1L, List(11)),
        Traversal("33_55_110", 1L, List(11)),
        Traversal("33_55_121", 1L, List(11)),
        Traversal("33_55_66", 1L, List(11)),
        Traversal("33_55_77", 1L, List(11)),
        Traversal("33_55_88", 1L, List(11)),
        Traversal("33_55_99", 1L, List(11)),
        Traversal("33_66_110", 1L, List(11)),
        Traversal("33_66_121", 1L, List(11)),
        Traversal("33_66_77", 1L, List(11)),
        Traversal("33_66_88", 1L, List(11)),
        Traversal("33_66_99", 1L, List(11)),
        Traversal("33_77_110", 1L, List(11)),
        Traversal("33_77_121", 1L, List(11)),
        Traversal("33_77_88", 1L, List(11)),
        Traversal("33_77_99", 1L, List(11)),
        Traversal("33_88_110", 1L, List(11)),
        Traversal("33_88_121", 1L, List(11)),
        Traversal("33_88_99", 1L, List(11)),
        Traversal("33_99_110", 1L, List(11)),
        Traversal("33_99_121", 1L, List(11)),
        Traversal("44_110_121", 1L, List(11)),
        Traversal("44_55_110", 1L, List(11)),
        Traversal("44_55_121", 1L, List(11)),
        Traversal("44_55_66", 1L, List(11)),
        Traversal("44_55_77", 1L, List(11)),
        Traversal("44_55_88", 1L, List(11)),
        Traversal("44_55_99", 1L, List(11)),
        Traversal("44_66_110", 1L, List(11)),
        Traversal("44_66_121", 1L, List(11)),
        Traversal("44_66_77", 1L, List(11)),
        Traversal("44_66_88", 1L, List(11)),
        Traversal("44_66_99", 1L, List(11)),
        Traversal("44_77_110", 1L, List(11)),
        Traversal("44_77_121", 1L, List(11)),
        Traversal("44_77_88", 1L, List(11)),
        Traversal("44_77_99", 1L, List(11)),
        Traversal("44_88_110", 1L, List(11)),
        Traversal("44_88_121", 1L, List(11)),
        Traversal("44_88_99", 1L, List(11)),
        Traversal("44_99_110", 1L, List(11)),
        Traversal("44_99_121", 1L, List(11)),
        Traversal("55_110_121", 1L, List(11)),
        Traversal("55_66_110", 1L, List(11)),
        Traversal("55_66_121", 1L, List(11)),
        Traversal("55_66_77", 1L, List(11)),
        Traversal("55_66_88", 1L, List(11)),
        Traversal("55_66_99", 1L, List(11)),
        Traversal("55_77_110", 1L, List(11)),
        Traversal("55_77_121", 1L, List(11)),
        Traversal("55_77_88", 1L, List(11)),
        Traversal("55_77_99", 1L, List(11)),
        Traversal("55_88_110", 1L, List(11)),
        Traversal("55_88_121", 1L, List(11)),
        Traversal("55_88_99", 1L, List(11)),
        Traversal("55_99_110", 1L, List(11)),
        Traversal("55_99_121", 1L, List(11)),
        Traversal("66_110_121", 1L, List(11)),
        Traversal("66_77_110", 1L, List(11)),
        Traversal("66_77_121", 1L, List(11)),
        Traversal("66_77_88", 1L, List(11)),
        Traversal("66_77_99", 1L, List(11)),
        Traversal("66_88_110", 1L, List(11)),
        Traversal("66_88_121", 1L, List(11)),
        Traversal("66_88_99", 1L, List(11)),
        Traversal("66_99_110", 1L, List(11)),
        Traversal("66_99_121", 1L, List(11)),
        Traversal("77_110_121", 1L, List(11)),
        Traversal("77_88_110", 1L, List(11)),
        Traversal("77_88_121", 1L, List(11)),
        Traversal("77_88_99", 1L, List(11)),
        Traversal("77_99_110", 1L, List(11)),
        Traversal("77_99_121", 1L, List(11)),
        Traversal("88_110_121", 1L, List(11)),
        Traversal("88_99_110", 1L, List(11)),
        Traversal("88_99_121", 1L, List(11)),
        Traversal("99_110_121", 1L, List(11))
      )
    }
  }

}
