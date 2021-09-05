package in.tap.we.poli.analytic.jobs.dynamo.traversal

import in.tap.base.spark.io.{Formats, In, Out}
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.BaseSparkJobSpec
import in.tap.we.poli.analytic.jobs.dynamo.autocomplete.VertexNameAutoCompleteJobFixtures
import in.tap.we.poli.analytic.jobs.dynamo.traversal.nx.InitJob
import in.tap.we.poli.analytic.jobs.dynamo.traversal.nx.InitJob.DstId
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.{Dataset, SaveMode}
import org.scalatest.DoNotDiscover

@DoNotDiscover
class InitJobSpec extends BaseSparkJobSpec with VertexNameAutoCompleteJobFixtures {

  it should "init from edges" in {
    val resourcePath: String = {
      getClass.getResource("../../../../../../../../dynamo/").toString
    }
    val inPath: String = {
      s"$resourcePath/graph_traversal/init/in/"
    }
    val outPath: String = {
      s"$resourcePath/graph_traversal/init/out/"
    }
    import spark.implicits._
    val _: Unit = {
      aggregateExpenditureEdges
        .toDS()
        .write
        .mode(SaveMode.Overwrite)
        .parquet(inPath)
      new InitJob(
        OneInArgs(In(inPath, Formats.PARQUET)),
        OneOutArgs(Out(outPath, Formats.PARQUET))
      ).execute()
    }
    val init: Dataset[(VertexId, DstId)] = {
      val ds: Dataset[(VertexId, DstId)] = {
        spark
          .read
          .parquet(outPath)
          .as[(VertexId, DstId)]
      }
      ds
    }
    init
      .collect()
      .toSeq
      .sortBy {
        case (srcId, dstId) =>
          (srcId, dstId.dst_id)
      } shouldBe {
      Seq(
        (1L, DstId(dst_id = 2L, analytics = analytics(101))),
        (2L, DstId(dst_id = 1L, analytics = analytics(101))),
        (11L, DstId(dst_id = 22L, analytics = analytics(10))),
        (11L, DstId(dst_id = 33L, analytics = analytics(11))),
        (11L, DstId(dst_id = 44L, analytics = analytics(12))),
        (11L, DstId(dst_id = 55L, analytics = analytics(13))),
        (11L, DstId(dst_id = 66L, analytics = analytics(1))),
        (11L, DstId(dst_id = 77L, analytics = analytics(2))),
        (11L, DstId(dst_id = 88L, analytics = analytics(3))),
        (11L, DstId(dst_id = 99L, analytics = analytics(22))),
        (11L, DstId(dst_id = 110L, analytics = analytics(23))),
        (11L, DstId(dst_id = 121L, analytics = analytics(99))),
        (22L, DstId(dst_id = 11L, analytics = analytics(10))),
        (33L, DstId(dst_id = 11L, analytics = analytics(11))),
        (44L, DstId(dst_id = 11L, analytics = analytics(12))),
        (55L, DstId(dst_id = 11L, analytics = analytics(13))),
        (66L, DstId(dst_id = 11L, analytics = analytics(1))),
        (77L, DstId(dst_id = 11L, analytics = analytics(2))),
        (88L, DstId(dst_id = 11L, analytics = analytics(3))),
        (99L, DstId(dst_id = 11L, analytics = analytics(22))),
        (110L, DstId(dst_id = 11L, analytics = analytics(23))),
        (121L, DstId(dst_id = 11L, analytics = analytics(99)))
      )
    }
  }

}
