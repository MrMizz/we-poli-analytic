package in.tap.we.poli.analytic.jobs.connectors.fuzzy.predictor

import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.VendorsFuzzyConnectorFeaturesJob.Comparator
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
          edge = emptyEdge.copy(src_id = 11L)
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
          edge = emptyEdge.copy(src_id = 11L)
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

}
