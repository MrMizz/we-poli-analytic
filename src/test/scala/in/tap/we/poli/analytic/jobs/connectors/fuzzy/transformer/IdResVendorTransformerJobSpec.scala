package in.tap.we.poli.analytic.jobs.connectors.fuzzy.transformer

import in.tap.base.spark.io.{Formats, In, Out}
import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.TwoOutArgs
import in.tap.we.poli.analytic.jobs.BaseSparkJobSpec
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.transfomer.IdResVendorTransformerJob
import org.apache.spark.sql.SaveMode
import org.scalatest.DoNotDiscover

@DoNotDiscover
class IdResVendorTransformerJobSpec extends BaseSparkJobSpec with IdResVendorTransformerJobFixtures {

  val resourcePath: String = {
    getClass.getResource("../../../../../../../../../connectors/vendors/fuzzy").toString
  }

  val in1Path: String = {
    s"$resourcePath/transformer/in1/"
  }

  val in2Path: String = {
    s"$resourcePath/transformer/in2/"
  }

  val out1Path: String = {
    s"$resourcePath/features/in1/"
  }

  val out2Path: String = {
    s"$resourcePath/features/in3/"
  }

  val _: Unit = {
    import spark.implicits._
    vendors.toDS.write.mode(SaveMode.Overwrite).parquet(in1Path)
    uniqueVendors.toDS.write.mode(SaveMode.Overwrite).parquet(in2Path)
    new IdResVendorTransformerJob(
      TwoInArgs(In(in1Path, Formats.PARQUET), In(in2Path, Formats.PARQUET)),
      TwoOutArgs(Out(out1Path, Formats.PARQUET), Out(out2Path, Formats.PARQUET))
    ).execute()
  }

}
