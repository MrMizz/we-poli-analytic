package in.tap.we.poli.analytic.jobs.connectors.fuzzy.predictor

import in.tap.we.poli.analytic.jobs.connectors.fuzzy.transfomer.IdResVendorTransformerJob.IdResVendor
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.ExpenditureEdge
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.{Address, Vendor}

trait VendorsFuzzyPredictorJobFixtures {

  lazy val vendor1: IdResVendor = {
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
        edge = emptyEdge.copy(src_id = 11L)
      )
    )
  }

  lazy val vendor2: IdResVendor = {
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
        edge = emptyEdge.copy(src_id = 11L)
      )
    )
  }

  lazy val vendor3: IdResVendor = {
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
    )
  }

  lazy val vendor4: IdResVendor = {
    IdResVendor(
      Vendor(
        uid = 3L,
        name = "Tacos Rico",
        address = Address.empty,
        memo = None,
        edge = emptyEdge.copy(src_id = 44L)
      )
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

}
