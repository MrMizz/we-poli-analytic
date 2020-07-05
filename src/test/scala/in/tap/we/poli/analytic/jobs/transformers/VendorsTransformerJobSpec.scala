package in.tap.we.poli.analytic.jobs.transformers

import org.scalatest.{FlatSpec, Matchers}

class VendorsTransformerJobSpec extends FlatSpec with Matchers with VendorsTransformerJobFixtures {

  it should "build unique hashes" in {
    // valid name, city, state, zip
    vendor1.hashes shouldBe Seq("vendor's, inc. # 1_city1_state1", "vendors_city1_state1", "vendors_zip1")
    // missing name
    vendor2.hashes shouldBe Nil
  }

  it should "extract edges from expenditures report" in {
    import VendorsTransformerJob.Vendor
    Vendor.fromOperatingExpenditures(operatingExpenditures1).map(_.edge) shouldBe {
      Some(edge1)
    }

    Vendor.fromOperatingExpenditures(operatingExpenditures2).map(_.edge) shouldBe {
      Some(edge2)
    }
  }

}
