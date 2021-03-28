package in.tap.we.poli.analytic.jobs.connectors.fuzzy.transformer

import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.ExpenditureEdge
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.{Address, Vendor}

trait IdResVendorTransformerJobFixtures {

  val vendors: Seq[Vendor] = {
    Seq(
      vendor1,
      vendor2,
      vendor3
    )
  }

  val uniqueVendors: Seq[UniqueVendor] = {
    Seq(
      uniqueVendor1,
      uniqueVendor2,
      uniqueVendor3
    )
  }

  private lazy val vendor1: Vendor = {
    Vendor(
      uid = 1L,
      name = "Vendor1",
      address = Address.empty,
      memo = None,
      edge = emptyEdge
    )
  }

  private lazy val vendor2: Vendor = {
    vendor1.copy(
      uid = 2L,
      name = "Vendor2"
    )
  }

  private lazy val vendor3: Vendor = {
    vendor1.copy(
      uid = 3L,
      name = "Vendor3"
    )
  }

  private lazy val uniqueVendor1: UniqueVendor = {
    UniqueVendor(
      uid = 1L,
      uids = Seq(1L),
      name = "Vendor1",
      names = Set.empty,
      address = Address.empty,
      addresses = Set.empty,
      memos = Set.empty,
      edges = Set(emptyEdge),
      num_merged = 1L
    )
  }

  private lazy val uniqueVendor2: UniqueVendor = {
    uniqueVendor1.copy(
      uid = 2L,
      uids = Seq(2L),
      name = "Vendor2",
      edges = Set(emptyEdge, emptyEdge.copy(src_id = 22L))
    )
  }

  private lazy val uniqueVendor3: UniqueVendor = {
    uniqueVendor1.copy(
      uid = 3L,
      uids = Seq(3L),
      name = "Vendor3",
      edges = Set(emptyEdge)
    )
  }

  private lazy val emptyEdge: ExpenditureEdge = {
    ExpenditureEdge(
      src_id = 11L,
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
