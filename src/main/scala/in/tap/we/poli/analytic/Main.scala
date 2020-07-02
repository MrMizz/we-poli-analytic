package in.tap.we.poli.analytic

import in.tap.base.spark.jobs.composite.CompositeJob
import in.tap.base.spark.main.InArgs.{OneInArgs, TwoInArgs}
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.base.spark.main.{InArgs, OutArgs}
import in.tap.we.poli.analytic.jobs.attribution.VendorsAttributionJob
import in.tap.we.poli.analytic.jobs.connectors.VendorsConnectorJob
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob
import org.apache.spark.sql.SparkSession

object Main extends in.tap.base.spark.main.Main {

  override def job(step: String, inArgs: InArgs, outArgs: OutArgs)(implicit spark: SparkSession): CompositeJob = {
    step match {
      case "vendors-transformer" =>
        new VendorsTransformerJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "vendors-connector" =>
        new VendorsConnectorJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "vendors-merger" =>
        new VendorsMergerJob(inArgs.asInstanceOf[TwoInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "vendors-attribution" =>
        new VendorsAttributionJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case _ => throw new MatchError("Invalid Step!")
    }
  }

}
