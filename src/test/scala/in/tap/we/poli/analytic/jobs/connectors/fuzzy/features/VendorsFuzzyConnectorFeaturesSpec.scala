package in.tap.we.poli.analytic.jobs.connectors.fuzzy.features

import in.tap.we.poli.analytic.jobs.BaseSpec
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.Features.CompositeFeatures
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.VendorsFuzzyConnectorFeaturesJob.buildSamplingRatio

class VendorsFuzzyConnectorFeaturesSpec extends BaseSpec with VendorsFuzzyConnectorFeaturesFixtures {

  it should "build comparisons from vendors" in {
    Comparison(Nil) shouldBe {
      Nil
    }
    Comparison(List(vendor1)) shouldBe {
      Nil
    }
    Comparison(List(vendor1, vendor2)) shouldBe {
      List(
        Comparison(
          vendor1,
          vendor2
        )
      )
    }
    Comparison(List(vendor1, vendor2, vendor3)) shouldBe {
      Seq(
        Comparison(
          vendor1,
          vendor2
        ),
        Comparison(
          vendor1,
          vendor3
        ),
        Comparison(
          vendor2,
          vendor3
        )
      )
    }
  }

  it should "build feature space from vendor comparison" in {
    // identity comparison
    Comparison(vendor1, vendor1).compositeFeatures shouldBe {
      CompositeFeatures(
        nameScore = 0.9239983170288842,
        addressScore = 0.6537911840313039,
        transactionScore = 0.9680266749215013
      )
    }
    // only name token in common
    Comparison(vendor1, vendor2).compositeFeatures shouldBe {
      CompositeFeatures(
        nameScore = 0.9239983170288842,
        addressScore = 0.004349657879857062,
        transactionScore = 0.17727112552700772
      )
    }
    // nothing in common
    Comparison(vendor2, vendor3).compositeFeatures shouldBe {
      CompositeFeatures(
        nameScore = 0.09692866798203056,
        addressScore = 0.004349657879857062,
        transactionScore = 0.17727112552700772
      )
    }
  }

  it should "sample positive label features in ratio to negative labels" in {
    buildSamplingRatio(10.0, 5.0) shouldBe 0.5
    buildSamplingRatio(10.0, 10.0) shouldBe 1.0
    buildSamplingRatio(10.0, 3.0) shouldBe 0.3
  }

}
