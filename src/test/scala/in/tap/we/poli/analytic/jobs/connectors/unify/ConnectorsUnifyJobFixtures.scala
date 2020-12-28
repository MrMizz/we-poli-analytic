package in.tap.we.poli.analytic.jobs.connectors.unify

import in.tap.we.poli.analytic.jobs.connectors.Connection

trait ConnectorsUnifyJobFixtures {

  val connection1A: Connection = {
    (1L, 1L)
  }

  val connection2A: Connection = {
    (2L, 1L)
  }

  val connection3A: Connection = {
    (3L, 2L)
  }

  val connection4A: Connection = {
    (4L, 3L)
  }

  val connection1B: Connection = {
    (1L, 4L)
  }

  val connection2B: Connection = {
    (3L, 5L)
  }

  val connection3B: Connection = {
    (4L, 4L)
  }

  val connection4B: Connection = {
    (5L, 6L)
  }

  val connectorA: Seq[Connection] = {
    Seq(
      connection1A,
      connection2A,
      connection3A,
      connection4A
    )
  }

  val connectorB: Seq[Connection] = {
    Seq(
      connection1B,
      connection2B,
      connection3B,
      connection4B
    )
  }

}
