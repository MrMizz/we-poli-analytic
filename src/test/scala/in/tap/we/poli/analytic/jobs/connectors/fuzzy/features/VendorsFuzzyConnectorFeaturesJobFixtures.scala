package in.tap.we.poli.analytic.jobs.connectors.fuzzy.features

import org.apache.spark.graphx.VertexId

trait VendorsFuzzyConnectorFeaturesJobFixtures {

  val connector: Seq[(Long, VertexId)] = {
    Seq(
      1L -> 101L,
      2L -> 101L,
      3L -> 101L
    )
  }

}
