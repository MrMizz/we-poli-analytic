package in.tap.we.poli.analytic.jobs.connectors.fuzzy.features

import in.tap.we.poli.analytic.jobs.BaseSpec
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.VendorsFuzzyConnectorFeaturesJob.{
  buildSamplingRatio, Comparison, Features
}

class VendorsFuzzyConnectorFeaturesJobSpec extends BaseSpec with VendorsFuzzyConnectorFeaturesJobFixtures {

  it should "build comparisons from vendors" in {
    Comparison(Nil) shouldBe {
      Nil
    }
    Comparison(Seq(vendor1)) shouldBe {
      Nil
    }
    Comparison(Seq(vendor1, vendor2)) shouldBe {
      Seq(
        Comparison(vendor1, vendor2)
      )
    }
    Comparison(Seq(vendor1, vendor2, vendor3)) shouldBe {
      Seq(
        Comparison(vendor1, vendor2),
        Comparison(vendor1, vendor3),
        Comparison(vendor2, vendor3)
      )
    }
  }

  it should "build feature space from vendor comparison" in {
    // identity comparison
    Comparison(vendor1, vendor1).features shouldBe {
      Features(
        1.0, 1.0, 1.0, 1.0, 1.0, 1.0
      )
    }
    // only name token in common
    Comparison(vendor1, vendor2).features shouldBe {
      Features(
        1.0, 1.0, 0.0, 0.0, 0.0, 0.0
      )
    }
  }

  it should "build feature space from unique vendor comparison" in {
    // identity comparison
    Comparison(uniqueVendor1, uniqueVendor1).features shouldBe {
      Features(
        1.0, 1.0, 2.0, 1.0, 1.0, 1.0
      )
    }
    // name token & edges in common
    Comparison(uniqueVendor1, uniqueVendor2).features shouldBe {
      Features(
        1.0, 1.0, 2.0, 0.0, 0.0, 0.0
      )
    }
    // only name token in common
    Comparison(uniqueVendor1, uniqueVendor3).features shouldBe {
      Features(
        1.0, 1.0, 0.0, 0.0, 0.0, 0.0
      )
    }
  }

  it should "sample positive label features in ratio to negative labels" in {
    buildSamplingRatio(10.0, 5.0) shouldBe 0.5
    buildSamplingRatio(10.0, 10.0) shouldBe 1.0
    buildSamplingRatio(10.0, 3.0) shouldBe 0.3
  }

}
