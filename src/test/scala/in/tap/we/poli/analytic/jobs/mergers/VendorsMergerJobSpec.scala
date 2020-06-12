package in.tap.we.poli.analytic.jobs.mergers

import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor._
import org.scalatest.{FlatSpec, Matchers}

class VendorsMergerJobSpec extends FlatSpec with Matchers with VendorsMergerJobFixtures {

  it should "build a unique hash from vendors" in {
    // valid data
    buildHash(vendor1) shouldBe Some("vendor1_city1_state1")
    // missing name
    buildHash(vendor2) shouldBe None
  }

  it should "reduce two vendors found to be the same entity" in {
    reduce(fromVendor(vendor1), fromVendor(vendor3)) shouldBe {
      UniqueVendor(
        sub_ids = Seq(1L, 3L),
        name = Some("Vendor1"),
        names = Seq("Vendor1", "Vendor3"),
        city = Some("City1"),
        state = Some("State1"),
        zip_code = Some("Zip1")
      )
    }
  }

}
