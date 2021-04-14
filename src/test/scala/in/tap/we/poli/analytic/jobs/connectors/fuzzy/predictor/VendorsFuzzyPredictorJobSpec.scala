package in.tap.we.poli.analytic.jobs.connectors.fuzzy.predictor

import in.tap.we.poli.analytic.jobs.BaseSpec
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.Comparison
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.Features.CompositeFeatures

class VendorsFuzzyPredictorJobSpec extends BaseSpec with VendorsFuzzyPredictorJobFixtures {

  it should "produce monotonically increasing predictions" in {
    val prediction1: Double = {
      Prediction(
        CompositeFeatures(
          nameScore = 1.0,
          addressScore = 0.0,
          transactionScore = 0.0
        )
      )
    }
    val prediction2: Double = {
      Prediction(
        CompositeFeatures(
          nameScore = 1.0,
          addressScore = 1.0,
          transactionScore = 0.0
        )
      )
    }
    val prediction3: Double = {
      Prediction(
        CompositeFeatures(
          nameScore = 1.0,
          addressScore = 1.0,
          transactionScore = 1.0
        )
      )
    }
    println("Monotonically Increasing")
    Seq(
      prediction1,
      prediction2,
      prediction3
    ).foreach(println)
    assert(prediction1 < prediction2)
    assert(prediction2 < prediction3)
  }

  it should "build predictions from vendor comparisons" in {
    // identity
    Prediction(
      Comparison(
        vendor1,
        vendor1
      )
    ) shouldBe {
      0.9973305733180393
    }
    // normalized as identity
    Prediction(
      Comparison(
        vendor1,
        vendor2
      )
    ) shouldBe {
      0.9973305733180393
    }
    // some in common
    Prediction(
      Comparison(
        vendor1,
        vendor3
      )
    ) shouldBe {
      0.623136164502322
    }
    // nothing in common
    Prediction(
      Comparison(
        vendor3,
        vendor4
      )
    ) shouldBe {
      4.3556972363955224E-5
    }
  }

}
