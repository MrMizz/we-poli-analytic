package in.tap.we.poli.analytic.jobs

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.FilerVendorTransactions
import in.tap.we.poli.models.OperatingExpenditures
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class FilerVendorTransactionsJob(override val inArgs: OneInArgs, override val outArgs: OneOutArgs)(
  implicit spark: SparkSession,
  val readTypeTagA: universe.TypeTag[OperatingExpenditures],
  val writeTypeTagA: universe.TypeTag[FilerVendorTransactions]
) extends OneInOneOutJob[OperatingExpenditures, FilerVendorTransactions](inArgs, outArgs) {

  override def transform(input: Dataset[OperatingExpenditures]): Dataset[FilerVendorTransactions] = {
    import spark.implicits._

    val transactionsWithKey: Dataset[((String, String), FilerVendorTransactions)] = {
      input
        .flatMap(FilerVendorTransactions.fromOperatingExpenditures)
        .map { transaction: FilerVendorTransactions =>
          transaction.filerId -> transaction.vendorName -> transaction
        }
    }

    val aggTransactions: Dataset[FilerVendorTransactions] = {
      transactionsWithKey
        .rdd
        .reduceByKey(FilerVendorTransactions.reduce)
        .map { case (_, transaction: FilerVendorTransactions) => transaction }
    }.toDS

    aggTransactions
  }

}
