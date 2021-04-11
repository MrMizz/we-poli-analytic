package in.tap.we.poli.analytic.jobs.connectors.fuzzy.features

import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.Features.{
  AddressFeatures, CompositeFeatures, NameFeatures
}
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.predictor.Prediction
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.transfomer.IdResVendorTransformerJob.IdResVendor

final case class Comparison(
  left_side: Comparator,
  right_side: Comparator
) {

  lazy val compositeFeatures: CompositeFeatures = {
    CompositeFeatures(
      sameSrcId = toDouble(sameSrcId),
      nameScore = Prediction(nameFeatures),
      addressScore = Prediction(addressFeatures)
    )
  }

  lazy val nameFeatures: NameFeatures = {
    NameFeatures(
      numTokens = numTokens,
      numTokensInCommon = numTokensInCommon
    )
  }

  lazy val addressFeatures: AddressFeatures = {
    AddressFeatures(
      sameZip = toDouble(sameZip),
      sameCity = toDouble(sameCity),
      sameState = toDouble(sameState)
    )
  }

  private lazy val numTokens: Double = {
    Seq(left_side.nameTokens.size, right_side.nameTokens.size).max.toDouble
  }

  // TODO: Validate for city/state
  private lazy val numTokensInCommon: Double = {
    left_side.nameTokens.intersect(right_side.nameTokens).size.toDouble
  }

  private lazy val sameSrcId: Boolean = {
    left_side.vendor.src_id.equals(right_side.vendor.src_id)
  }

  private lazy val sameZip: Boolean = {
    same(_.address.zip_code)
  }

  private lazy val sameCity: Boolean = {
    same(_.address.city)
  }

  private lazy val sameState: Boolean = {
    same(_.address.state)
  }

  private def toDouble(bool: Boolean): Double = {
    bool.compare(false)
  }

  private def same(f: IdResVendor => Option[String]): Boolean = {
    (f(left_side.vendor), f(right_side.vendor)) match {
      case (Some(left), Some(right)) =>
        left.equals(right)
      case _ =>
        false
    }
  }

}

object Comparison {

  implicit class Syntax(comparison: Comparison) {

    val left: IdResVendor = {
      comparison.left_side.vendor
    }

    val right: IdResVendor = {
      comparison.right_side.vendor
    }

  }

  def apply(list: List[Comparator]): List[Comparison] = {
    combinations(list).map {
      case (left, right) =>
        Comparison(
          left_side = left,
          right_side = right
        )
    }
  }

  private def combinations[A](list: List[A]): List[(A, A)] = {
    list.combinations(n = 2).toList.flatMap { combination: Seq[A] =>
      combination match {
        case left :: right :: Nil =>
          Some((left, right))
        case _ =>
          None
      }
    }
  }

}
