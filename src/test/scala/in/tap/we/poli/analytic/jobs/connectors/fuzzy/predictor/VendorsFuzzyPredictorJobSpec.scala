package in.tap.we.poli.analytic.jobs.connectors.fuzzy.predictor

import in.tap.we.poli.analytic.jobs.BaseSpec
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.Comparison
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.Features.CompositeFeatures

class VendorsFuzzyPredictorJobSpec extends BaseSpec with VendorsFuzzyPredictorJobFixtures {

  it should "produce monotonically increasing predictions" in {
    val prediction1: Double = {
      Prediction(
        CompositeFeatures(
          sameSrcId = 1.0,
          nameScore = 0.0,
          addressScore = 0.0
        )
      )
    }
    val prediction2: Double = {
      Prediction(
        CompositeFeatures(
          sameSrcId = 1.0,
          nameScore = 1.0,
          addressScore = 0.0
        )
      )
    }
    val prediction3: Double = {
      Prediction(
        CompositeFeatures(
          sameSrcId = 1.0,
          nameScore = 1.0,
          addressScore = 1.0
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
      0.9099752510053672
    }
    // normalized as identity
    Prediction(
      Comparison(
        vendor1,
        vendor2
      )
    ) shouldBe {
      0.9099752510053672
    }
    // some in common
    Prediction(
      Comparison(
        vendor1,
        vendor3
      )
    ) shouldBe {
      0.6552687199583234
    }
    // nothing in common
    Prediction(
      Comparison(
        vendor3,
        vendor4
      )
    ) shouldBe {
      0.5007986237526968
    }
  }

}
