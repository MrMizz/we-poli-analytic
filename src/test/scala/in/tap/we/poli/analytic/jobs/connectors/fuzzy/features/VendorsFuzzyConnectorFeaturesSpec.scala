package in.tap.we.poli.analytic.jobs.connectors.fuzzy.features

import in.tap.we.poli.analytic.jobs.BaseSpec
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.VendorsFuzzyConnectorFeaturesJob.{
  buildSamplingRatio, Comparison, Features
}

class VendorsFuzzyConnectorFeaturesSpec extends BaseSpec with VendorsFuzzyConnectorFeaturesFixtures {

  it should "build comparisons from vendors" in {
    Comparison.buildFromVendors(Nil) shouldBe {
      Nil
    }
    Comparison.buildFromVendors(List(vendor1)) shouldBe {
      Nil
    }
    Comparison.buildFromVendors(List(vendor1, vendor2)) shouldBe {
      List(
        Comparison(
          vendor1,
          vendor2,
          2.0,
          2.0
        )
      )
    }
    Comparison.buildFromVendors(List(vendor1, vendor2, vendor3)) shouldBe {
      Seq(
        Comparison(
          vendor1,
          vendor2,
          3.0,
          3.0
        ),
        Comparison(
          vendor1,
          vendor3,
          3.0,
          3.0
        ),
        Comparison(
          vendor2,
          vendor3,
          3.0,
          3.0
        )
      )
    }
  }

  it should "build comparisons from unique vendors" in {
    Comparison.buildFromUniqueVendors(Nil) shouldBe {
      Nil
    }
    Comparison.buildFromUniqueVendors(List(uniqueVendor1)) shouldBe {
      Nil
    }
    Comparison.buildFromUniqueVendors(List(uniqueVendor1, uniqueVendor2)) shouldBe {
      List(
        Comparison(
          uniqueVendor1,
          uniqueVendor2,
          3.0,
          3.0
        )
      )
    }
    Comparison.buildFromUniqueVendors(List(uniqueVendor1, uniqueVendor2, uniqueVendor3, uniqueVendor4)) shouldBe {
      Seq(
        Comparison(
          uniqueVendor1,
          uniqueVendor2,
          3.0,
          3.0
        ),
        Comparison(
          uniqueVendor1,
          uniqueVendor3,
          4.0,
          3.0
        ),
        Comparison(
          uniqueVendor1,
          uniqueVendor4,
          4.0,
          0.0
        ),
        Comparison(
          uniqueVendor2,
          uniqueVendor3,
          4.0,
          3.0
        ),
        Comparison(
          uniqueVendor2,
          uniqueVendor4,
          4.0,
          0.0
        ),
        Comparison(
          uniqueVendor3,
          uniqueVendor4,
          5.0,
          0.0
        )
      )
    }
  }

  it should "build feature space from vendor comparison" in {
    // identity comparison
    Comparison(vendor1, vendor1, 1.0, 1.0).features shouldBe {
      Features(
        1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0
      )
    }
    // only name token in common
    Comparison(vendor1, vendor2, 0.0, 0.0).features shouldBe {
      Features(
        1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0
      )
    }
  }

  it should "build feature space from unique vendor comparison" in {
    // identity comparison
    Comparison(uniqueVendor1, uniqueVendor1, 3.0, 3.0).features shouldBe {
      Features(
        1.0, 1.0, 3.0, 3.0, 1.0, 1.0, 1.0
      )
    }
    // name token & edges in common
    Comparison(uniqueVendor1, uniqueVendor2, 3.0, 3.0).features shouldBe {
      Features(
        1.0, 1.0, 3.0, 3.0, 0.0, 0.0, 0.0
      )
    }
    // only name token in common
    Comparison(uniqueVendor1, uniqueVendor4, 4.0, 0.0).features shouldBe {
      Features(
        1.0, 1.0, 4.0, 0.0, 0.0, 0.0, 0.0
      )
    }
  }

  it should "sample positive label features in ratio to negative labels" in {
    buildSamplingRatio(10.0, 5.0) shouldBe 0.5
    buildSamplingRatio(10.0, 10.0) shouldBe 1.0
    buildSamplingRatio(10.0, 3.0) shouldBe 0.3
  }

}
