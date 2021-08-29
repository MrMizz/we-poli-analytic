package in.tap.we.poli.analytic.jobs.dynamo.traversal

trait GraphTraversalJobFixtures {

  val traversal1: Seq[Long] = {
    Seq.fill(99)(22L)
  }

  val traversal2: Seq[Long] = {
    Seq.fill(101)(22L)
  }

  val traversal3: Seq[Long] = {
    val seq: Seq[Long] = {
      (0 to 299 by 1).map { i =>
        i.toLong
      }
    }
    seq
  }

}
