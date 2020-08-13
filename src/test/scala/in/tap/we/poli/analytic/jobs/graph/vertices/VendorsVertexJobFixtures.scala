package in.tap.we.poli.analytic.jobs.graph.vertices

import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.ExpenditureEdge
import in.tap.we.poli.analytic.jobs.graph.vertices.VendorsVertexJob.VendorVertex
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor

trait VendorsVertexJobFixtures {

  val uniqueVendor1: UniqueVendor = {
    new UniqueVendor(
      uid = 1L,
      uids = Seq(1L),
      name = "Mickey's Consulting",
      names = Set("Mickey's Consulting"),
      city = Some("Los Angeles"),
      state = None,
      zip_code = None,
      memos = Set("payroll", "campaign consulting"),
      num_merged = 1,
      edges = Set(edge1)
    )
  }

  val uniqueVendor2: UniqueVendor = {
    new UniqueVendor(
      uid = 2L,
      uids = Seq(2L, 3L, 4L),
      name = "Domino's",
      names = Set("Domino's"),
      city = Some("Los Angeles"),
      state = Some("CA"),
      zip_code = None,
      memos = Set("food", "dinner"),
      num_merged = 3,
      edges = Set(edge2, edge3, edge4)
    )
  }

  val uniqueVendor3: UniqueVendor = {
    new UniqueVendor(
      uid = 5L,
      uids = Seq(5L),
      name = "Raphael Saadiq",
      names = Set("Raphael Saadiq"),
      city = Some("Los Angeles"),
      state = Some("CA"),
      zip_code = None,
      memos = Set("media consulting"),
      num_merged = 1,
      edges = Set(edge5)
    )
  }

  val vendorVertex1: VendorVertex = {
    VendorVertex(
      uid = 1L,
      name = "Mickey's Consulting",
      city = Some("Los Angeles"),
      zip = None,
      state = None,
      has_been_affiliated = Some(true),
      has_been_consultant = Some(true),
      has_been_staff = Some(true)
    )
  }

  val vendorVertex2: VendorVertex = {
    VendorVertex(
      uid = 2L,
      name = "Domino's",
      city = Some("Los Angeles"),
      zip = None,
      state = Some("CA"),
      has_been_affiliated = None,
      has_been_consultant = None,
      has_been_staff = None
    )
  }

  val vendorVertex3: VendorVertex = {
    VendorVertex(
      uid = 5L,
      name = "Raphael Saadiq",
      city = Some("Los Angeles"),
      zip = None,
      state = Some("CA"),
      has_been_affiliated = Some(true),
      has_been_consultant = Some(true),
      has_been_staff = None
    )
  }

  lazy val edge1: ExpenditureEdge = {
    ExpenditureEdge(
      src_id = 6L,
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
      src_id = 7L,
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
      src_id = 8L,
      report_year = Some(2010L),
      report_type = Some("report type 3"),
      form_type = Some("form type 3"),
      schedule_type = Some("schedule type 3"),
      transaction_date = Some("transaction date 3"),
      transaction_amount = Some(303.303d),
      primary_general_indicator = Some("pgi 3"),
      disbursement_category = Some("disbursement category 3"),
      entity_type = Some("entity type 3"),
      transaction_id = Some("transaction id 3"),
      back_reference_transaction_number = Some("back reference transaction number 3")
    )
  }

  lazy val edge4: ExpenditureEdge = {
    ExpenditureEdge(
      src_id = 8L,
      report_year = Some(2010L),
      report_type = Some("report type 4"),
      form_type = Some("form type 4"),
      schedule_type = Some("schedule type 4"),
      transaction_date = Some("transaction date 4"),
      transaction_amount = Some(404.404d),
      primary_general_indicator = Some("pgi 4"),
      disbursement_category = Some("disbursement category 4"),
      entity_type = None,
      transaction_id = None,
      back_reference_transaction_number = None
    )
  }

  lazy val edge5: ExpenditureEdge = {
    ExpenditureEdge(
      src_id = 10L,
      report_year = Some(2010L),
      report_type = Some("report type 5"),
      form_type = Some("form type 5"),
      schedule_type = Some("schedule type 5"),
      transaction_date = None,
      transaction_amount = None,
      primary_general_indicator = Some("pgi 5"),
      disbursement_category = Some("disbursement category 5"),
      entity_type = None,
      transaction_id = Some("transaction id 5"),
      back_reference_transaction_number = Some("back reference transaction number 5")
    )
  }

}
