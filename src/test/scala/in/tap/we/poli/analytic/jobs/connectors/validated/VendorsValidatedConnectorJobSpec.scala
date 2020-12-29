package in.tap.we.poli.analytic.jobs.connectors.validated

import in.tap.base.spark.io.{In, Out}
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.BaseSparkJobSpec
import in.tap.we.poli.analytic.jobs.connectors.Connection
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.SaveMode

class VendorsValidatedConnectorJobSpec extends BaseSparkJobSpec with VendorsValidatedConnectorJobFixtures {

  val resourcePath: String = {
    getClass.getResource("../../../../../../../../connectors/vendors/validated").toString
  }

  val inPath: String = {
    s"$resourcePath/in/"
  }

  val outPath: String = {
    s"$resourcePath/out/"
  }

  val _: Unit = {
    import spark.implicits._
    validations.toDS.write.mode(SaveMode.Overwrite).json(inPath)
    new VendorsValidatedConnectorJob(
      OneInArgs(In(inPath)),
      OneOutArgs(Out(outPath))
    ).execute()
  }

  it should "connect validations" in {
    import spark.implicits._
    val results: Seq[(Long, Seq[VertexId])] = {
      spark
        .read
        .json(outPath)
        .as[Connection]
        .collect
        .toSeq
        .groupBy(_._2)
        .values
        .toSeq
        .map { groupedTup: Seq[(VertexId, VertexId)] =>
          val grouped: Seq[VertexId] = groupedTup.map(_._1)
          grouped.sum -> grouped.sorted
        }
        .sortBy(_._1)
    }
    results shouldBe {
      Seq(
        (4L, Seq(4L)),
        (6L, Seq(1L, 2L, 3L)),
        (17L, Seq(8L, 9L)),
        (18L, Seq(5L, 6L, 7L))
      )
    }
  }

}
