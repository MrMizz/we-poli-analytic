package in.tap.we.poli.analytic.jobs.connectors.fuzzy.features

import in.tap.base.spark.io.{Formats, In, Out}
import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.BaseSparkJobSpec
import org.apache.spark.sql.SaveMode
import org.scalatest.DoNotDiscover

@DoNotDiscover
class VendorsFuzzyConnectorFeaturesJobSpec extends BaseSparkJobSpec with VendorsFuzzyConnectorFeaturesJobFixtures {

  val resourcePath: String = {
    getClass.getResource("../../../../../../../../../connectors/vendors/fuzzy/features").toString
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
    connector.toDS.write.mode(SaveMode.Overwrite).parquet(in2Path)
    new VendorsFuzzyConnectorFeaturesJob(
      TwoInArgs(In(in1Path, Formats.PARQUET), In(in2Path, Formats.PARQUET)),
      OneOutArgs(Out(outPath, Formats.PARQUET))
    ).execute()
  }

}
