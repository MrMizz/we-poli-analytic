package in.tap.we.poli.analytic.jobs.connectors.fuzzy.predictor

import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.ExpenditureEdge
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.Address

trait VendorsFuzzyPredictorJobFixtures {

  lazy val uniqueVendor1: UniqueVendor = {
    UniqueVendor(
      uid = 1L,
      uids = Nil,
      name = "Vendor1",
      names = Set.empty,
      address = Address
        .empty
        .copy(
          city = Some("Los Angeles"),
          state = Some("CA"),
          zip_code = Some("90026")
        ),
      addresses = Set.empty[Address],
      memos = Set.empty,
      edges = edgesInCommon,
      num_merged = 3L
    )
  }

  lazy val uniqueVendor2: UniqueVendor = {
    UniqueVendor(
      uid = 2L,
      uids = Nil,
      name = "Vendor2",
      names = Set.empty,
      address = Address
        .empty
        .copy(
          city = Some("los angeles"),
          state = Some("ca"),
          zip_code = Some("90026")
        ),
      addresses = Set.empty[Address],
      memos = Set.empty,
      edges = edgesInCommon,
      num_merged = 3L
    )
  }

  lazy val uniqueVendor3: UniqueVendor = {
    UniqueVendor(
      uid = 3L,
      uids = Nil,
      name = "Vendor3",
      names = Set.empty,
      address = Address
        .empty
        .copy(
          city = Some("Santa Barbara"),
          state = Some("CA")
        ),
      addresses = Set.empty[Address],
      memos = Set.empty,
      edges = Set(emptyEdge),
      num_merged = 1L
    )
  }

  lazy val edgesInCommon: Set[ExpenditureEdge] = {
    Set(emptyEdge.copy(src_id = 11L), emptyEdge.copy(src_id = 22L), emptyEdge.copy(src_id = 33L))
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
