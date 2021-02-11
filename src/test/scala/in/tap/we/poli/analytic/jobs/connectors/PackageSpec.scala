package in.tap.we.poli.analytic.jobs.connectors

import in.tap.we.poli.analytic.jobs.BaseSpec

class PackageSpec extends BaseSpec {

  it should "clean & tokenize names" in {
    cleanedNameTokens("My Favorite Campaign & Tacos") shouldBe {
      Seq("my", "favorite", "campaign", "tacos")
    }
    cleanedNameTokens("My Favorite       Campaign & Tacos") shouldBe {
      Seq("my", "favorite", "campaign", "tacos")
    }
    cleanedNameTokens("My Favorite Campaign && Tacos  !!!!") shouldBe {
      Seq("my", "favorite", "campaign", "tacos")
    }
    cleanedNameTokens("my FavoritE Campaign, LLC. ") shouldBe {
      Seq("my", "favorite", "campaign")
    }
    cleanedNameTokens("my Favorite Campaign, INC.!") shouldBe {
      Seq("my", "favorite", "campaign")
    }
  }

  it should "clean names" in {
    cleanedName("Berlin Rosen Ltd") shouldBe {
      "berlinrosen"
    }
    cleanedName("Berling Rosen") shouldBe {
      "berlingrosen"
    }
  }

  it should "produce string similarity metrics" in {
    stringSimilarity("My Favorite campaign", "my Favorite Campaign, INC.!") shouldBe {
      Some(0.8555555555555555)
    }
    stringSimilarity("Berlin Rosen Ltd", "Berling Rosen") shouldBe {
      Some(0.93461538461538468)
    }
    stringSimilarity("common wealth press", "commonwealth press") shouldBe {
      Some(0.956140350877193)
    }
  }

  it should "produce name similarity metrics" in {
    nameSimilarity("My Favorite campaign", "my Favorite Campaign, INC.!") shouldBe {
      Some(1.0)
    }
    nameSimilarity("Berlin Rosen Ltd", "Berling Rosen") shouldBe {
      Some(0.9833333333333333)
    }
    nameSimilarity("common wealth press", "commonwealth press") shouldBe {
      Some(1.0)
    }
  }

}
