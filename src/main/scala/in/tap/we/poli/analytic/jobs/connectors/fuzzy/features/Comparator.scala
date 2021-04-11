package in.tap.we.poli.analytic.jobs.connectors.fuzzy.features

import in.tap.we.poli.analytic.jobs.connectors.cleanedNameTokens
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.transfomer.IdResVendorTransformerJob.IdResVendor
import org.apache.spark.graphx.VertexId

final case class Comparator(
  vendor: IdResVendor,
  ccid: VertexId
) {

  val nameTokens: Set[String] = {
    cleanedNameTokens(vendor.name).toSet
  }

  val addressTokens: Set[String] = {
    Set(vendor.address.city, vendor.address.zip_code).flatten
  }

  val cgTokens: Set[String] = {
    nameTokens ++ addressTokens
  }

}
