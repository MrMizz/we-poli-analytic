package in.tap.we.poli.analytic.jobs.mergers

import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.Vendor

trait VendorsMergerJobFixtures {

  val vendor1: Vendor = {
    new Vendor(
      uid = 1L,
      name = Some("Vendor1"),
      city = Some("City1"),
      state = Some("State1"),
      zip_code = Some("Zip1")
    )
  }

  val vendor2: Vendor = {
    new Vendor(
      uid = 2L,
      name = None,
      city = Some("City1"),
      state = Some("State1"),
      zip_code = Some("Zip1")
    )
  }

  val vendor3: Vendor = {
    new Vendor(
      uid = 3L,
      name = Some("Vendor3"),
      city = Some("City1"),
      state = Some("State1"),
      zip_code = Some("Zip1")
    )
  }

}
