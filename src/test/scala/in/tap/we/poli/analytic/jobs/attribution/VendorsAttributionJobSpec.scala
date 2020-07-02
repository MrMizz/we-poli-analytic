package in.tap.we.poli.analytic.jobs.attribution

import org.scalatest.{FlatSpec, Matchers}

class VendorsAttributionJobSpec extends FlatSpec with Matchers with VendorsAttributionJobFixtures {

  it should "attribute a vendor into a vertex" in {
    import VendorsAttributionJob._

    VendorVertex.fromVendor(uniqueVendor1) shouldBe {
      1L -> VendorVertex(
        uid = 1L,
        name = Some("Mickey's Consulting"),
        city = Some("Los Angeles"),
        zip = None,
        state = None,
        has_been_affiliated = Some(true),
        has_been_consultant = Some(true),
        has_been_staff = Some(true)
      )
    }

    VendorVertex.fromVendor(uniqueVendor2) shouldBe {
      2L -> VendorVertex(
        uid = 2L,
        name = Some("Domino's"),
        city = Some("Los Angeles"),
        zip = None,
        state = Some("CA"),
        has_been_affiliated = None,
        has_been_consultant = None,
        has_been_staff = None
      )
    }

    VendorVertex.fromVendor(uniqueVendor3) shouldBe {
      5L -> VendorVertex(
        uid = 5L,
        name = Some("Raphael Saadiq"),
        city = Some("Los Angeles"),
        zip = None,
        state = Some("CA"),
        has_been_affiliated = Some(true),
        has_been_consultant = Some(true),
        has_been_staff = None
      )
    }
  }

}
