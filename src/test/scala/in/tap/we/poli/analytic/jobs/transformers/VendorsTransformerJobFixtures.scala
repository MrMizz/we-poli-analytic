package in.tap.we.poli.analytic.jobs.transformers

import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.Vendor

trait VendorsTransformerJobFixtures {

  val vendor1: Vendor = {
    new Vendor(
      uid = 1L,
      name = Some("Vendor's, Inc. # 1"),
      city = Some("City1"),
      state = Some("State1"),
      zip_code = Some("Zip1"),
      memo = None
    )
  }

  val vendor2: Vendor = {
    new Vendor(
      uid = 2L,
      name = None,
      city = Some("City1"),
      state = Some("State1"),
      zip_code = Some("Zip1"),
      memo = None
    )
  }

}
