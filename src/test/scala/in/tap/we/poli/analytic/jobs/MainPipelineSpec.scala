package in.tap.we.poli.analytic.jobs

import in.tap.we.poli.analytic.jobs.connectors.auto.VendorsAutoConnectorJobSpec
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJobSpec
import org.scalatest.Suites

class MainPipelineSpec extends Suites(new VendorsAutoConnectorJobSpec, new VendorsMergerJobSpec)
