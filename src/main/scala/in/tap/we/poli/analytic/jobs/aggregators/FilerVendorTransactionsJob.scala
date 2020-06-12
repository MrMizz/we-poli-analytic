package in.tap.we.poli.analytic.jobs.aggregators

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.aggregators.FilerVendorTransactionsJob._
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

object FilerVendorTransactionsJob {

  /**
   * Result of GroupBy of Filing Committee -> Vendor.
   *
   * @param filerId CMTE_ID Filer identification number
   * @param vendorName NAME Contributor/Lender/Transfer Name
   * @param totalSpend Sum of TRANSACTION_AMT Transaction amount
   * @param numTransactions Count of total transactions between pair
   */
  final case class FilerVendorTransactions(
    filerId: String,
    vendorName: String,
    totalSpend: Double,
    numTransactions: Int
  )

  object FilerVendorTransactions {

    def fromOperatingExpenditures(operatingExpenditures: OperatingExpenditures): Option[FilerVendorTransactions] = {
      for {
        vendorName <- operatingExpenditures.NAME
        totalSpend <- operatingExpenditures.TRANSACTION_AMT
      } yield {
        FilerVendorTransactions(
          filerId = operatingExpenditures.CMTE_ID,
          vendorName = vendorName,
          totalSpend = totalSpend,
          numTransactions = 1
        )
      }
    }

    def reduce(left: FilerVendorTransactions, right: FilerVendorTransactions): FilerVendorTransactions = {
      left.copy(
        totalSpend = left.totalSpend + right.totalSpend,
        numTransactions = left.numTransactions + right.numTransactions
      )
    }

  }

}
