package in.tap.we.poli.analytic.jobs.connectors.fuzzy.transformer

import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.ExpenditureEdge
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.{Address, Vendor}

trait IdResVendorTransformerJobFixtures {

  val vendors: Seq[Vendor] = {
    Seq(
      vendor1,
      vendor2,
      vendor3
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
