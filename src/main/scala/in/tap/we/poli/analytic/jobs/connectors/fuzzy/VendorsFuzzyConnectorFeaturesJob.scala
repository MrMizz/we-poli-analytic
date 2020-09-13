package in.tap.we.poli.analytic.jobs.connectors.fuzzy

import in.tap.we.poli.analytic.jobs.connectors.ConnectorUtils
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.Vendor

class VendorsFuzzyConnectorFeaturesJob {}

object VendorsFuzzyConnectorFeaturesJob {

  final case class Comparator(
    vendor: Vendor
  ) {

    val nameTokens: Set[String] = {
      ConnectorUtils.cleanedNameTokens(vendor.name).toSet
    }

  }

  final case class Comparison(
    left_side: Comparator,
    right_side: Comparator
  ) {

    val numTokens: Double = {
      Seq(left_side.nameTokens.size, right_side.nameTokens.size).max.toDouble
    }

    val numTokensInCommon: Double = {
      left_side.nameTokens.intersect(right_side.nameTokens).size.toDouble
    }

    val sameCity: Option[Boolean] = {
      for {
        leftCity <- left_side.vendor.city.map(_.toLowerCase)
        rightCity <- right_side.vendor.city.map(_.toLowerCase)
      } yield {
        leftCity.equals(rightCity)
      }
    }



    private def toDouble(maybeBool: Option[Boolean]): Double = {
      maybeBool.map(_.compare(false)).getOrElse(0d)
    }

  }

}
