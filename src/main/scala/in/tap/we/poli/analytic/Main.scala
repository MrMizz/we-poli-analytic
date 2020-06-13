package in.tap.we.poli.analytic

import in.tap.base.spark.jobs.composite.CompositeJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.base.spark.main.{InArgs, OutArgs}
import in.tap.we.poli.analytic.jobs.aggregators.{CommitteesAggregateJob, FilerVendorTransactionsJob}
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob
import org.apache.spark.sql.SparkSession

object Main extends in.tap.base.spark.main.Main {

  override def job(step: String, inArgs: InArgs, outArgs: OutArgs)(implicit spark: SparkSession): CompositeJob = {
    step match {
      case "transactions-group-by" =>
        new FilerVendorTransactionsJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "cmte-agg" =>
        new CommitteesAggregateJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "vendors-transformer" =>
        new VendorsTransformerJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "vendors-merger" =>
        new VendorsMergerJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case _ => throw new MatchError("Invalid Step!")
    }
  }

}
