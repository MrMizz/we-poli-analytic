package in.tap.we.poli.analytic.jobs.connectors

import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.ExpenditureEdge
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.Vendor

trait VendorsConnectorJobFixtures {

  val vendor1: Vendor = {
    new Vendor(
      uid = 1L,
      name = Some("Vendor, Inc. # 1"),
      city = Some("City1"),
      state = Some("State1"),
      zip_code = Some("Zip1"),
      memo = Some("memo1"),
      edge = edge1
    )
  }

  val vendor2: Vendor = {
    new Vendor(
      uid = 2L,
      name = None,
      city = Some("City1"),
      state = Some("State1"),
      zip_code = Some("Zip1"),
      memo = None,
      edge = edge2
    )
  }

  val vendor3: Vendor = {
    new Vendor(
      uid = 3L,
      name = Some("Vendor"),
      city = Some("City1"),
      state = Some("State1"),
      zip_code = Some("Zip1"),
      memo = None,
      edge = edge3
    )
  }

  lazy val edge1: ExpenditureEdge = {
    ExpenditureEdge(
      src_id = 1L,
      report_year = Some(2010L),
      report_type = Some("report type 1"),
      form_type = Some("form type 1"),
      schedule_type = Some("schedule type 1"),
      transaction_date = Some("transaction date 1"),
      transaction_amount = Some(101.101d),
      primary_general_indicator = Some("pgi 1"),
      disbursement_category = Some("disbursement category 1"),
      entity_type = Some("entity type 1"),
      transaction_id = Some("transaction id 1"),
      back_reference_transaction_number = Some("back reference transaction number 1")
    )
  }

  lazy val edge2: ExpenditureEdge = {
    ExpenditureEdge(
      src_id = 2L,
      report_year = Some(2010L),
      report_type = Some("report type 2"),
      form_type = Some("form type 2"),
      schedule_type = Some("schedule type 2"),
      transaction_date = Some("transaction date 2"),
      transaction_amount = Some(202.202d),
      primary_general_indicator = Some("pgi 2"),
      disbursement_category = Some("disbursement category 2"),
      entity_type = Some("entity type 2"),
      transaction_id = Some("transaction id 2"),
      back_reference_transaction_number = Some("back reference transaction number 2")
    )
  }

  lazy val edge3: ExpenditureEdge = {
    ExpenditureEdge(
      src_id = 3L,
      report_year = Some(2010L),
      report_type = Some("report type 3"),
      form_type = Some("form type 3"),
      schedule_type = Some("schedule type 3"),
      transaction_date = None,
      transaction_amount = None,
      primary_general_indicator = Some("pgi 3"),
      disbursement_category = Some("disbursement category 3"),
      entity_type = None,
      transaction_id = Some("transaction id 3"),
      back_reference_transaction_number = Some("back reference transaction number 3")
    )
  }

}
