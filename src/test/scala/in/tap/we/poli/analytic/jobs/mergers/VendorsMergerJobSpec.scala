package in.tap.we.poli.analytic.jobs.mergers

import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor._
import org.scalatest.{FlatSpec, Matchers}

class VendorsMergerJobSpec extends FlatSpec with Matchers with VendorsMergerJobFixtures {

  it should "reduce two vendors found to be the same entity" in {
    reduce(fromVendor(vendor1), fromVendor(vendor2)) shouldBe {
      UniqueVendor(
        uid = 1L,
        uids = Seq(1L, 2L),
        name = Some("Vendor1"),
        names = Seq("Vendor1", "Vendor2"),
        city = Some("City1"),
        state = Some("State1"),
        zip_code = Some("Zip1"),
        num_merged = 2
      )
    }
  }

}
