package in.tap.we.poli.analytic.jobs.connectors.auto

import in.tap.base.spark.io.{Formats, In, Out}
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.BaseSparkJobSpec
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.Vendor
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.Dataset
import org.scalatest.DoNotDiscover

@DoNotDiscover
class VendorsAutoConnectorJobSpec extends BaseSparkJobSpec with VendorsAutoConnectorJobFixtures {

  val resourcePath: String = {
    getClass.getResource("../../../../../../../../").toString
  }

  val connectorResourcePath: String = {
    s"$resourcePath/connectors/vendors/auto/"
  }

  val mergerPath: String = {
    s"$resourcePath/mergers/vendors/"
  }

  val inPath: String = {
    s"$connectorResourcePath/in"
  }

  val outPath: String = {
    s"$connectorResourcePath/out"
  }

  val _: Unit = {
    import org.apache.spark.sql.SaveMode
    import spark.implicits._
    // build input resource
    val vendors: Dataset[Vendor] = Seq(
      vendor1,
      vendor2,
      vendor3
    ).toDS
    // write input resource to connector dir
    vendors
      .write
      .mode(SaveMode.Overwrite)
      .parquet(inPath)
    // write input resource to merger dir
    vendors
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"$mergerPath/in1/")
    // run job & write to output resource
    new VendorsAutoConnectorJob(
      OneInArgs(In(path = inPath, format = Formats.PARQUET)),
      OneOutArgs(Out(path = outPath, format = Formats.JSON))
    ).execute()
    // write output resource to merger dir
    spark
      .read
      .json(outPath)
      .write
      .mode(SaveMode.Overwrite)
      .json(s"$mergerPath/in2/")
  }

  it should "connect vendors" in {
    import spark.implicits._
    spark.read.json(outPath).as[(VertexId, VertexId)].collect.toSeq.sortBy(_._1) shouldBe {
      Seq(
        1L -> 1L,
        2L -> 2L,
        3L -> 1L
      )
    }
  }

}
