package in.tap.we.poli.analytic.jobs.connectors.unify

import in.tap.base.spark.io.{In, Out}
import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.BaseSparkJobSpec
import in.tap.we.poli.analytic.jobs.connectors.Connection
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.SaveMode

class UniqueVendorConnectorFlattenJobSpec extends BaseSparkJobSpec with UniqueVendorConnectorFlattenJobFixtures {

  val resourcePath: String = {
    getClass.getResource("../../../../../../../../connectors/vendors/flatten").toString
  }

  val in1Path: String = {
    s"$resourcePath/in1/"
  }

  val in2Path: String = {
    s"$resourcePath/in2/"
  }

  val outPath: String = {
    s"$resourcePath/out/"
  }

  val _: Unit = {
    import spark.implicits._
    uniqueVendors.toDS.write.mode(SaveMode.Overwrite).json(in1Path)
    connector.toDS.write.mode(SaveMode.Overwrite).json(in2Path)
    new UniqueVendorConnectorFlattenJob(
      TwoInArgs(In(in1Path), In(in2Path)),
      OneOutArgs(Out(outPath))
    ).execute()
  }

  it should "flatten a unique vendor connector" in {
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
        (3L, Seq(3L)),
        (18L, Seq(1L, 2L, 4L, 5L, 6L))
      )
    }
  }

}
