package in.tap.we.poli.analytic

import in.tap.base.spark.jobs.composite.CompositeJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.base.spark.main.{InArgs, OutArgs}
import in.tap.we.poli.analytic.jobs.FilerVendorTransactionsJob
import org.apache.spark.sql.SparkSession

object Main extends in.tap.base.spark.main.Main {

  override def job(step: String, inArgs: InArgs, outArgs: OutArgs)(implicit spark: SparkSession): CompositeJob = {
    step match {
      case "transactions-group-by" =>
        new FilerVendorTransactionsJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
    }
  }

}
