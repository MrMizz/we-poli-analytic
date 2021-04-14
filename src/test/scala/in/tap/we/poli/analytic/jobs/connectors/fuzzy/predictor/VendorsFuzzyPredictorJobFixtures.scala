package in.tap.we.poli.analytic.jobs.connectors.fuzzy.predictor

import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.Comparator
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.transfomer.IdResVendorTransformerJob.IdResVendor
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.ExpenditureEdge
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.{Address, Vendor}

trait VendorsFuzzyPredictorJobFixtures {

  lazy val vendor1: Comparator = {
    Comparator(
      IdResVendor(
        Vendor(
          uid = 1L,
          name = "Vendor1",
          address = Address
            .empty
            .copy(
              city = Some("los angeles"),
              state = Some("ca"),
              zip_code = Some("90026")
            ),
          memo = None,
          edge = edge.copy(src_id = 11L)
        )
      ),
      901L
    )
  }

  lazy val vendor2: Comparator = {
    Comparator(
      IdResVendor(
        Vendor(
          uid = 2L,
          name = "Vendor2",
          address = Address
            .empty
            .copy(
              city = Some("los angeles"),
              state = Some("ca"),
              zip_code = Some("90026")
            ),
          memo = None,
          edge = edge.copy(src_id = 11L)
        )
      ),
      901L
    )
  }

  lazy val vendor3: Comparator = {
    Comparator(
      IdResVendor(
        Vendor(
          uid = 3L,
          name = "Vendor3",
          address = Address
            .empty
            .copy(
              city = Some("santa barbara"),
              state = Some("ca")
            ),
          memo = None,
          edge = emptyEdge.copy(src_id = 33L)
        )
      ),
      902L
    )
  }

  lazy val vendor4: Comparator = {
    Comparator(
      IdResVendor(
        Vendor(
          uid = 3L,
          name = "Tacos Rico",
          address = Address.empty,
          memo = None,
          edge = emptyEdge.copy(src_id = 44L)
        )
      ),
      903L
    )
  }

  lazy val emptyEdge: ExpenditureEdge = {
    ExpenditureEdge(
      src_id = -11L,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None
    )
  }

  lazy val edge: ExpenditureEdge = {
    ExpenditureEdge(
      src_id = 111L,
      report_year = Some(2010L),
      report_type = Some("report type 1"),
      form_type = Some("form type 1"),
      transaction_amount = Some(100.0),
      disbursement_category = Some("disbursement category 1"),
      entity_type = Some("entity type 1"),
      schedule_type = None,
      transaction_date = None,
      primary_general_indicator = None,
      transaction_id = None,
      back_reference_transaction_number = None
    )
  }

}
