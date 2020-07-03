package in.tap.we.poli.analytic.jobs.graph.vertices

import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor

trait VendorsVertexJobFixtures {

  val uniqueVendor1: UniqueVendor = {
    new UniqueVendor(
      uid = 1L,
      uids = Seq(1L),
      name = Some("Mickey's Consulting"),
      names = Set("Mickey's Consulting"),
      city = Some("Los Angeles"),
      state = None,
      zip_code = None,
      memos = Set("payroll", "campaign consulting"),
      num_merged = 1
    )
  }

  val uniqueVendor2: UniqueVendor = {
    new UniqueVendor(
      uid = 2L,
      uids = Seq(2L, 3L, 4L),
      name = Some("Domino's"),
      names = Set("Domino's"),
      city = Some("Los Angeles"),
      state = Some("CA"),
      zip_code = None,
      memos = Set("food", "dinner"),
      num_merged = 3
    )
  }

  val uniqueVendor3: UniqueVendor = {
    new UniqueVendor(
      uid = 5L,
      uids = Seq(5L),
      name = Some("Raphael Saadiq"),
      names = Set("Raphael Saadiq"),
      city = Some("Los Angeles"),
      state = Some("CA"),
      zip_code = None,
      memos = Set("media consulting"),
      num_merged = 1
    )
  }

}
