package in.tap.we.poli.analytic.jobs.transformers

import in.tap.we.poli.analytic.jobs.BaseSpec

class VendorsTransformerJobSpec extends BaseSpec with VendorsTransformerJobFixtures {

  it should "build unique hashes" in {
    // valid name, city, state, zip
    vendor1.hashes shouldBe Seq("vendor's, inc. # 1_city1_state1", "vendors_city1_state1", "vendors_zip1")
    // missing name
    vendor2.hashes shouldBe Nil
  }

  it should "extract edges from expenditures report" in {
    import VendorsTransformerJob.Vendor
    Vendor.fromOperatingExpenditures(operatingExpenditures1).map(_.edges) shouldBe {
      Some(Set(edge1))
    }

    Vendor.fromOperatingExpenditures(operatingExpenditures2).map(_.edges) shouldBe {
      None
    }
  }

}
