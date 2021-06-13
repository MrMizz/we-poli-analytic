package in.tap.we.poli.analytic.jobs.mergers

import in.tap.we.poli.analytic.jobs.BaseSpec

class PackageSpec extends BaseSpec {

  it should "get most common" in {
    val l1 = {
      Seq(1, 2, 3, 2)
    }
    val l2 = {
      Seq(1)
    }
    val l3 = {
      Nil
    }
    val l4 = {
      Seq(1, 2, 1, 2, 3)
    }
    getMostCommon(l1) shouldBe {
      Some(2)
    }
    getMostCommon(l2) shouldBe {
      Some(1)
    }
    getMostCommon(l3) shouldBe {
      None
    }
    getMostCommon(l4) shouldBe {
      Some(2)
    }
  }

  it should "set freq" in {
    val m1 = {
      Map(
        "a" -> 1L,
        "b" -> 1L,
        "c" -> 2L,
        "d" -> 3L,
        "e" -> 1L
      )
    }
    val m2 = {
      Map(
        "d" -> 2L,
        "e" -> 3L,
        "f" -> 1L
      )
    }
    val union = {
      Map(
        "a" -> 1L,
        "b" -> 1L,
        "c" -> 2L,
        "d" -> 5L,
        "e" -> 4L,
        "f" -> 1L
      )
    }
    val empty = {
      Map.empty[String, Long]
    }
    // put
    SetFreq.put("f")(m1) shouldBe {
      m1.updated("f", 1L)
    }
    SetFreq.put("c")(m1) shouldBe {
      m1.updated("c", 3L)
    }
    // reduce
    SetFreq.reduce(m1, m2) shouldBe {
      union
    }
    SetFreq.reduce(m2, m1) shouldBe {
      union
    }
    SetFreq.reduce(m1, empty) shouldBe {
      m1
    }
    SetFreq.reduce(empty, m2) shouldBe {
      m2
    }
    SetFreq.reduce(empty, empty) shouldBe {
      empty
    }
    // get most common
    SetFreq.getMostCommon(m1) shouldBe {
      Some("d")
    }
    SetFreq.getMostCommon(empty) shouldBe {
      None
    }
  }

}
