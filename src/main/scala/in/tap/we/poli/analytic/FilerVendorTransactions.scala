package in.tap.we.poli.analytic

import in.tap.we.poli.models.OperatingExpenditures

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
