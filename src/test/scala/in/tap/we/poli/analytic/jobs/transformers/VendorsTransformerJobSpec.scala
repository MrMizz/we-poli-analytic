package in.tap.we.poli.analytic.jobs.transformers

import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJobFixtures
import org.scalatest.{FlatSpec, Matchers}

class VendorsTransformerJobSpec extends FlatSpec with Matchers with VendorsMergerJobFixtures {

  it should "build a unique hash" in {
    // valid data
    vendor1.hash1 shouldBe Some("vendor1_city1_state1")
    // missing name
    vendor2.hash1 shouldBe None
  }

}
