package in.tap.we.poli.analytic.jobs.connectors.unify

import in.tap.we.poli.analytic.jobs.connectors.Connection
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.ExpenditureEdge
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.Address

trait UniqueVendorConnectorFlattenJobFixtures {

  val uniqueVendor1: UniqueVendor = {
    val address = {
      Address
        .empty
        .copy(
          city = Some("Los Angeles"),
          state = Some("CA"),
          zip_code = Some("90041")
        )
    }
    UniqueVendor(
      uid = 1L,
      uids = Seq(1L, 2L),
      name = "vendor1",
      names = Set("vendor1"),
      name_freq = Map("vendor1" -> 1L),
      address = address,
      addresses = Set(address),
      address_freq = Map(address -> 1L),
      memos = Set("memo"),
      edges = Set(
        ExpenditureEdge(
          src_id = 2L,
          report_year = Some(1L),
          report_type = Some("a"),
          form_type = Some("a"),
          schedule_type = Some("a"),
          transaction_date = Some("a"),
          transaction_amount = Some(1.0),
          primary_general_indicator = Some("a"),
          disbursement_category = Some("a"),
          entity_type = Some("a"),
          transaction_id = Some("a"),
          back_reference_transaction_number = Some("a")
        )
      ),
      num_merged = 2
    )
  }

  val uniqueVendor2: UniqueVendor = {
    uniqueVendor1.copy(
      uid = 3L,
      uids = Seq(3L),
      name = "vendor3",
      num_merged = 1
    )
  }

  val uniqueVendor3: UniqueVendor = {
    uniqueVendor1.copy(
      uid = 4L,
      uids = Seq(4L, 5L, 6L),
      name = "vendor4",
      num_merged = 3
    )
  }

  val uniqueVendors: Seq[UniqueVendor] = {
    Seq(
      uniqueVendor1,
      uniqueVendor2,
      uniqueVendor3
    )
  }

  val connector: Seq[Connection] = {
    Seq(
      (1L, 1L),
      (3L, 2L),
      (4L, 1L)
    )
  }

}
