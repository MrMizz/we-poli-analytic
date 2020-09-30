package in.tap.we.poli.analytic.jobs.connectors.fuzzy

import in.tap.we.poli.analytic.jobs.BaseSpec
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.VendorsFuzzyConnectorFeaturesJob.{
  Comparator, Features, UniqueVendorComparison, VendorComparison, buildSamplingRatio
}

class VendorsFuzzyConnectorFeaturesJobSpec extends BaseSpec with VendorsFuzzyConnectorFeaturesJobFixtures {

  it should "build comparisons from vendors" in {
    VendorComparison(Nil) shouldBe {
      Nil
    }
    VendorComparison(Seq(vendor1)) shouldBe {
      Nil
    }
    VendorComparison(Seq(vendor1, vendor2)) shouldBe {
      Seq(
        VendorComparison(Comparator(vendor1), Comparator(vendor2), 2)
      )
    }
    VendorComparison(Seq(vendor1, vendor2, vendor3)) shouldBe {
      Seq(
        VendorComparison(Comparator(vendor1), Comparator(vendor2), 3),
        VendorComparison(Comparator(vendor1), Comparator(vendor3), 3),
        VendorComparison(Comparator(vendor2), Comparator(vendor3), 3)
      )
    }
  }

  it should "build comparisons from unique vendors" in {
    UniqueVendorComparison(Nil) shouldBe {
      Nil
    }
    UniqueVendorComparison(Seq(Comparator(uniqueVendor1))) shouldBe {
      Nil
    }
    UniqueVendorComparison(Seq(Comparator(uniqueVendor1), Comparator(uniqueVendor2))) shouldBe {
      Seq(
        UniqueVendorComparison(Comparator(uniqueVendor1), Comparator(uniqueVendor2))
      )
    }
    UniqueVendorComparison(Seq(Comparator(uniqueVendor1), Comparator(uniqueVendor2), Comparator(uniqueVendor3))) shouldBe {
      Seq(
        UniqueVendorComparison(Comparator(uniqueVendor1), Comparator(uniqueVendor2)),
        UniqueVendorComparison(Comparator(uniqueVendor1), Comparator(uniqueVendor3)),
        UniqueVendorComparison(Comparator(uniqueVendor2), Comparator(uniqueVendor3))
      )
    }
  }

  it should "build feature space from vendor comparison" in {
    // identity comparison
    VendorComparison(Comparator(vendor1), Comparator(vendor1), 2).features shouldBe {
      Features(
        1.0, 1.0, 2.0, 1.0, 1.0, 1.0
      )
    }
    // only name token in common
    VendorComparison(Comparator(vendor1), Comparator(vendor2), 0).features shouldBe {
      Features(
        1.0, 1.0, 0.0, 0.0, 0.0, 0.0
      )
    }
  }

  it should "build feature space from unique vendor comparison" in {
    // identity comparison
    UniqueVendorComparison(Comparator(uniqueVendor1), Comparator(uniqueVendor1)).features shouldBe {
      Features(
        1.0, 1.0, 3.0, 1.0, 1.0, 1.0
      )
    }
    // name token & edges in common
    UniqueVendorComparison(Comparator(uniqueVendor1), Comparator(uniqueVendor2)).features shouldBe {
      Features(
        1.0, 1.0, 3.0, 0.0, 0.0, 0.0
      )
    }
    // only name token in common
    UniqueVendorComparison(Comparator(uniqueVendor1), Comparator(uniqueVendor3)).features shouldBe {
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
