package in.tap.we.poli.analytic.jobs.connectors.validated

import in.tap.we.poli.analytic.jobs.connectors.validated.VendorsValidatedConnectorJob.Validation

trait VendorsValidatedConnectorJobFixtures {

  val validation1: Validation = {
    Validation(l = 1L, r = 2L)
  }

  val validation2: Validation = {
    Validation(l = 1L, r = 3L)
  }

  val validation3: Validation = {
    Validation(l = 2L, r = 3L)
  }

  val validation4: Validation = {
    Validation(l = 4L, r = 4L)
  }

  val validation5: Validation = {
    Validation(l = 5L, r = 6L)
  }

  val validation6: Validation = {
    Validation(l = 6L, r = 5L)
  }

  val validation7: Validation = {
    Validation(l = 7L, r = 6L)
  }

  val validation8: Validation = {
    Validation(l = 8L, r = 9L)
  }

  val validations: Seq[Validation] = {
    Seq(
      validation1,
      validation2,
      validation3,
      validation4,
      validation5,
      validation6,
      validation7,
      validation8
    )
  }

}
