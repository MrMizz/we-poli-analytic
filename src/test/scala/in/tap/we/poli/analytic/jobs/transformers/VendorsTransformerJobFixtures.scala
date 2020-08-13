package in.tap.we.poli.analytic.jobs.transformers

import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.ExpenditureEdge
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.Vendor
import in.tap.we.poli.models.OperatingExpenditures

trait VendorsTransformerJobFixtures {

  val vendor1: Vendor = {
    new Vendor(
      uid = 1L,
      name = "Vendor's, Inc. # 1",
      city = Some("City1"),
      state = Some("State1"),
      zip_code = Some("Zip1"),
      memo = None,
      edge = edge1
    )
  }

  val vendor2: Vendor = {
    new Vendor(
      uid = 2L,
      name = "Vendor",
      city = Some("City1"),
      state = None,
      zip_code = None,
      memo = None,
      edge = edge2
    )
  }

  lazy val edge1: ExpenditureEdge = {
    ExpenditureEdge(
      src_id = 13L,
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
      src_id = 14L,
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

  val operatingExpenditures1: OperatingExpenditures = {
    OperatingExpenditures(
      CMTE_ID = "C3",
      AMNDT_IND = None,
      RPT_YR = Some(2010L),
      RPT_TP = Some("report type 1"),
      FORM_TP_CD = Some("form type 1"),
      SCHED_TP_CD = Some("schedule type 1"),
      NAME = Some("Vendor1"),
      CITY = None,
      STATE = None,
      ZIP_CODE = None,
      TRANSACTION_DT = Some("transaction date 1"),
      TRANSACTION_AMT = Some(101.101d),
      TRANSACTION_PGI = Some("pgi 1"),
      CATEGORY = Some("disbursement category 1"),
      CATEGORY_DESC = None,
      MEMO_CD = None,
      MEMO_TEXT = None,
      ENTITY_TP = Some("entity type 1"),
      SUB_ID = Some(1L),
      FILE_NUM = None,
      TRAN_ID = Some("transaction id 1"),
      BACK_REF_TRAN_ID = Some("back reference transaction number 1"),
      IMAGE_NUM = None,
      LINE_NUM = None,
      PURPOSE = None
    )
  }

  val operatingExpenditures2: OperatingExpenditures = {
    OperatingExpenditures(
      CMTE_ID = "C4",
      AMNDT_IND = None,
      RPT_YR = Some(2010L),
      RPT_TP = Some("report type 2"),
      FORM_TP_CD = Some("form type 2"),
      SCHED_TP_CD = Some("schedule type 2"),
      NAME = None,
      CITY = None,
      STATE = None,
      ZIP_CODE = None,
      TRANSACTION_DT = Some("transaction date 2"),
      TRANSACTION_AMT = Some(202.202d),
      TRANSACTION_PGI = Some("pgi 2"),
      CATEGORY = Some("disbursement category 2"),
      CATEGORY_DESC = None,
      MEMO_CD = None,
      MEMO_TEXT = None,
      ENTITY_TP = Some("entity type 2"),
      SUB_ID = Some(2L),
      FILE_NUM = None,
      TRAN_ID = Some("transaction id 2"),
      BACK_REF_TRAN_ID = Some("back reference transaction number 2"),
      IMAGE_NUM = None,
      LINE_NUM = None,
      PURPOSE = None
    )
  }

}
