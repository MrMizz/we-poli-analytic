package in.tap.we.poli.analytic.jobs.connectors.fuzzy.transformer

import in.tap.base.spark.io.{Formats, In, Out}
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.BaseSparkJobSpec
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.transfomer.IdResVendorTransformerJob
import org.apache.spark.sql.SaveMode
import org.scalatest.DoNotDiscover

@DoNotDiscover
class IdResVendorTransformerJobSpec extends BaseSparkJobSpec with IdResVendorTransformerJobFixtures {

  val resourcePath: String = {
    getClass.getResource("../../../../../../../../../connectors/vendors/fuzzy").toString
  }

  val inPath: String = {
    s"$resourcePath/transformer/in/"
  }

  val outPath: String = {
    s"$resourcePath/features/in1/"
  }

  val _: Unit = {
    import spark.implicits._
    vendors.toDS.write.mode(SaveMode.Overwrite).parquet(inPath)
    new IdResVendorTransformerJob(
      OneInArgs(In(inPath, Formats.PARQUET)),
      OneOutArgs(Out(outPath, Formats.PARQUET))
    ).execute()
  }

}
