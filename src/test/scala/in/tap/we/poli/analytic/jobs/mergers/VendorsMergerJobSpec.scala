package in.tap.we.poli.analytic.jobs.mergers

import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor._
import org.scalatest.{FlatSpec, Matchers}

class VendorsMergerJobSpec extends FlatSpec with Matchers with VendorsMergerJobFixtures {

  it should "reduce two vendors found to be the same entity" in {
    val (uniqueVendor, uidFromVendor1): (UniqueVendor, String) = {
      for {
        uniqueVendor1 <- fromVendor(vendor1)
        uniqueVendor3 <- fromVendor(vendor3)
        uidFromVendor1 <- vendor1.uid1
      } yield {
        reduce(uniqueVendor1, uniqueVendor3) -> uidFromVendor1
      }
    }.get

    uniqueVendor shouldBe {
      UniqueVendor(
        uid = uidFromVendor1,
        uids = Seq(vendor1.uid1, vendor3.uid1).flatten,
        sub_ids = Seq(1L, 3L),
        name = Some("Vendor1"),
        names = Seq("Vendor1", "Vendor3"),
        city = Some("City1"),
        state = Some("State1"),
        zip_code = Some("Zip1"),
        num_merged = 2
      )
    }
  }

}
