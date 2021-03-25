package in.tap.we.poli.analytic.jobs.connectors.fuzzy.features

import in.tap.we.poli.analytic.jobs.BaseSpec
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.VendorsFuzzyConnectorFeaturesJob.{
  buildSamplingRatio, Comparator, Comparison, Features
}

class VendorsFuzzyConnectorFeaturesJobSpec extends BaseSpec with VendorsFuzzyConnectorFeaturesJobFixtures {

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
          Comparator(vendor1),
          Comparator(vendor2),
          2
        )
      )
    }
    Comparison.buildFromVendors(List(vendor1, vendor2, vendor3)) shouldBe {
      Seq(
        Comparison(
          Comparator(vendor1),
          Comparator(vendor2),
          3
        ),
        Comparison(
          Comparator(vendor1),
          Comparator(vendor3),
          3
        ),
        Comparison(
          Comparator(vendor2),
          Comparator(vendor3),
          3
        )
      )
    }
  }

  // TODO: it should "build comparisons from unique vendors" in {

  it should "build feature space from vendor comparison" in {
    // identity comparison
    Comparison(Comparator(vendor1), Comparator(vendor1), 1).features shouldBe {
      Features(
        1.0, 1.0, 1.0, 1.0, 1.0, 1.0
      )
    }
    // only name token in common
    Comparison(Comparator(vendor1), Comparator(vendor2), 0).features shouldBe {
      Features(
        1.0, 1.0, 0.0, 0.0, 0.0, 0.0
      )
    }
  }

  it should "build feature space from unique vendor comparison" in {
    // identity comparison
    Comparison(Comparator(uniqueVendor1), Comparator(uniqueVendor1), 3).features shouldBe {
      Features(
        1.0, 1.0, 3.0, 1.0, 1.0, 1.0
      )
    }
    // name token & edges in common
    Comparison(Comparator(uniqueVendor1), Comparator(uniqueVendor2), 3).features shouldBe {
      Features(
        1.0, 1.0, 3.0, 0.0, 0.0, 0.0
      )
    }
    // only name token in common
    Comparison(Comparator(uniqueVendor1), Comparator(uniqueVendor3), 0).features shouldBe {
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
