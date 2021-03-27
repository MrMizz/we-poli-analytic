package in.tap.we.poli.analytic.jobs.connectors.fuzzy

import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.VendorsFuzzyConnectorFeaturesJobSpec
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.transformer.IdResVendorTransformerJobSpec
import org.scalatest.Suites

class IdResPipelineSpec extends Suites(new IdResVendorTransformerJobSpec, new VendorsFuzzyConnectorFeaturesJobSpec)
