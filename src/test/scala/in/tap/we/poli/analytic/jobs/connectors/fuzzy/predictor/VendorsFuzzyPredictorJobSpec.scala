package in.tap.we.poli.analytic.jobs.connectors.fuzzy.predictor

import in.tap.we.poli.analytic.jobs.BaseSpec
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.VendorsFuzzyConnectorFeaturesJob.{
  Comparator, Comparison, Features
}
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.predictor.VendorsFuzzyPredictorJob.Prediction
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.transfomer.IdResVendorTransformerJob

class VendorsFuzzyPredictorJobSpec extends BaseSpec with VendorsFuzzyPredictorJobFixtures {

  it should "produce monotonically increasing predictions" in {
    val prediction1: Double = {
      Prediction.predict(
        Features(
          numTokens = 1.0,
          numTokensInCommon = 1.0,
          numSrcIdsInCommon = 0.0,
          sameCity = 0.0,
          sameZip = 0.0,
          sameState = 0.0
        )
      )
    }
    val prediction2: Double = {
      Prediction.predict(
        Features(
          numTokens = 2.0,
          numTokensInCommon = 2.0,
          numSrcIdsInCommon = 0.0,
          sameCity = 0.0,
          sameZip = 0.0,
          sameState = 0.0
        )
      )
    }
    val prediction3: Double = {
      Prediction.predict(
        Features(
          numTokens = 2.0,
          numTokensInCommon = 2.0,
          numSrcIdsInCommon = 1.0,
          sameCity = 0.0,
          sameZip = 0.0,
          sameState = 0.0
        )
      )
    }
    val prediction4: Double = {
      Prediction.predict(
        Features(
          numTokens = 2.0,
          numTokensInCommon = 2.0,
          numSrcIdsInCommon = 2.0,
          sameCity = 0.0,
          sameZip = 0.0,
          sameState = 0.0
        )
      )
    }
    val prediction5: Double = {
      Prediction.predict(
        Features(
          numTokens = 2.0,
          numTokensInCommon = 2.0,
          numSrcIdsInCommon = 2.0,
          sameCity = 1.0,
          sameZip = 0.0,
          sameState = 0.0
        )
      )
    }
    val prediction6: Double = {
      Prediction.predict(
        Features(
          numTokens = 2.0,
          numTokensInCommon = 2.0,
          numSrcIdsInCommon = 2.0,
          sameCity = 1.0,
          sameZip = 1.0,
          sameState = 0.0
        )
      )
    }
    val prediction7: Double = {
      Prediction.predict(
        Features(
          numTokens = 2.0,
          numTokensInCommon = 2.0,
          numSrcIdsInCommon = 2.0,
          sameCity = 1.0,
          sameZip = 1.0,
          sameState = 1.0
        )
      )
    }
    println("Monotonically Increasing")
    Seq(
      prediction1,
      prediction2,
      prediction3,
      prediction4,
      prediction5,
      prediction6,
      prediction7
    ).foreach(println)
    assert(prediction1 < prediction2)
    assert(prediction2 < prediction3)
    assert(prediction3 < prediction4)
    assert(prediction4 < prediction5)
    assert(prediction5 < prediction6)
    assert(prediction6 < prediction7)
  }

  it should "also product non-monotonically increasing predictions" in {
    val prediction1: Double = {
      Prediction.predict(
        Features(
          numTokens = 1.0,
          numTokensInCommon = 1.0,
          numSrcIdsInCommon = 0.0,
          sameCity = 0.0,
          sameZip = 0.0,
          sameState = 0.0
        )
      )
    }
    val prediction2: Double = {
      Prediction.predict(
        Features(
          numTokens = 2.0,
          numTokensInCommon = 1.0,
          numSrcIdsInCommon = 0.0,
          sameCity = 0.0,
          sameZip = 0.0,
          sameState = 0.0
        )
      )
    }
    println("Non-Monotonically Increasing")
    Seq(
      prediction1,
      prediction2
    ).foreach(println)
    assert(prediction1 > prediction2)
  }

  it should "build predictions from unique vendor comparisons" in {
    // identity
    Prediction(
      Comparison(
        Comparator(
          IdResVendorTransformerJob.Source(uniqueVendor1)
        ),
        Comparator(
          IdResVendorTransformerJob.Source(uniqueVendor1)
        ),
        3
      )
    ) shouldBe {
      0.9999999933542082
    }
    // normalized as identity
    Prediction(
      Comparison(
        Comparator(
          IdResVendorTransformerJob.Source(uniqueVendor1)
        ),
        Comparator(
          IdResVendorTransformerJob.Source(uniqueVendor2)
        ),
        3
      )
    ) shouldBe {
      0.9999999933542082
    }
    // nothing in common
    Prediction(
      Comparison(
        Comparator(
          IdResVendorTransformerJob.Source(uniqueVendor1)
        ),
        Comparator(
          IdResVendorTransformerJob.Source(uniqueVendor3)
        ),
        0
      )
    ) shouldBe {
      1.756493588665972e-4
    }
  }

}
