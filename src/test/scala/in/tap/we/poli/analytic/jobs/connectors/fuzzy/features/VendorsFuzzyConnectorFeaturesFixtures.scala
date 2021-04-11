package in.tap.we.poli.analytic.jobs.connectors.fuzzy.features

import in.tap.we.poli.analytic.jobs.connectors.fuzzy.transfomer.IdResVendorTransformerJob.IdResVendor
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.ExpenditureEdge
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.{Address, Vendor}

trait VendorsFuzzyConnectorFeaturesFixtures {

  val vendor1: Comparator = {
    Comparator(
      IdResVendor(
        emptyVendor.copy(
          uid = 1L,
          name = "Vendor1",
          address = Address
            .empty
            .copy(
              city = Some("Los Angeles"),
              state = Some("CA"),
              zip_code = Some("90026")
            ),
          memo = None,
          edge = emptyEdge.copy(src_id = -111L)
        )
      ),
      900L
    )
  }

  val vendor2: Comparator = {
    Comparator(
      IdResVendor(
        emptyVendor.copy(
          uid = 2L,
          name = "Vendor2",
          edge = emptyEdge.copy(src_id = 22L)
        )
      ),
      901L
    )
  }

  val vendor3: Comparator = {
    Comparator(
      IdResVendor(
        emptyVendor.copy(
          uid = 3L,
          name = "Vendor3",
          edge = emptyEdge.copy(src_id = 33L)
        )
      ),
      902L
    )
  }

  lazy val emptyVendor: Vendor = {
    Vendor(
      uid = -1L,
      name = "",
      address = Address.empty,
      memo = None,
      edge = emptyEdge.copy(src_id = -111L)
    )
  }

  lazy val emptyUniqueVendor: UniqueVendor = {
    UniqueVendor(
      uid = -1L,
      uids = Nil,
      name = "",
      names = Set.empty,
      address = Address.empty,
      addresses = Set.empty[Address],
      memos = Set.empty,
      edges = Set.empty,
      num_merged = 0
    )
  }

  lazy val emptyEdge: ExpenditureEdge = {
    ExpenditureEdge(
      src_id = -111L,
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
