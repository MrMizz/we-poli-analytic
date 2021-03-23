package in.tap.we.poli.analytic.jobs.connectors.fuzzy.features

import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.VendorsFuzzyConnectorFeaturesJob.Comparator
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.transfomer.IdResVendorTransformerJob.IdResVendor
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.ExpenditureEdge
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.{Address, Vendor}

trait VendorsFuzzyConnectorFeaturesJobFixtures {

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
          edges = Set(emptyEdge.copy(src_id = -111L))
        )
      )
    )
  }

  val uniqueVendor1: Comparator = {
    Comparator(
      IdResVendor(
        UniqueVendor(
          uid = 1L,
          uids = Nil,
          name = "Vendor1",
          names = Set("Vendor1"),
          addresses = Set(
            Address
              .empty
              .copy(
                city = Some("Los Angeles"),
                state = Some("CA"),
                zip_code = Some("90026")
              )
          ),
          address = Address.empty,
          memos = Set.empty,
          edges = edgesInCommon,
          num_merged = 0
        )
      )
    )
  }

  val vendor2: Comparator = {
    Comparator(
      IdResVendor(
        emptyVendor.copy(
          uid = 2L,
          name = "Vendor2",
          edges = Set(emptyEdge.copy(src_id = 22L))
        )
      )
    )
  }

  val uniqueVendor2: Comparator = {
    Comparator(
      IdResVendor(
        emptyUniqueVendor.copy(
          uid = 2L,
          names = Set("Vendor2"),
          edges = edgesInCommon
        )
      )
    )
  }

  val uniqueVendor3: Comparator = {
    Comparator(
      IdResVendor(
        emptyUniqueVendor.copy(
          uid = 3L,
          names = Set("Vendor3")
        )
      )
    )
  }

  val vendor3: Comparator = {
    Comparator(
      IdResVendor(
        emptyVendor.copy(
          uid = 3L,
          name = "Vendor3",
          edges = Set(emptyEdge.copy(src_id = 33L))
        )
      )
    )
  }

  lazy val emptyVendor: Vendor = {
    Vendor(
      uid = -1L,
      name = "",
      address = Address.empty,
      memo = None,
      edges = Set(emptyEdge.copy(src_id = -111L))
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

  lazy val edgesInCommon: Set[ExpenditureEdge] = {
    Set(emptyEdge.copy(src_id = 11L), emptyEdge.copy(src_id = 22L), emptyEdge.copy(src_id = 33L))
  }

}