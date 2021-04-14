package in.tap.we.poli.analytic.jobs.connectors.fuzzy.features

import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.Features.{
  AddressFeatures, CompositeFeatures, NameFeatures, TransactionFeatures
}
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.predictor.Prediction
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.transfomer.IdResVendorTransformerJob.IdResVendor

final case class Comparison(
  left_side: Comparator,
  right_side: Comparator
) {

  lazy val compositeFeatures: CompositeFeatures = {
    CompositeFeatures(
      nameScore = Prediction(nameFeatures),
      addressScore = Prediction(addressFeatures),
      transactionScore = Prediction(transactionFeatures)
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
      sameCity = toDouble(sameCity),
      sameState = toDouble(sameState)
    )
  }

  lazy val transactionFeatures: TransactionFeatures = {
    TransactionFeatures(
      sameSrcId = toDouble(sameSrcId),
      reportYearDiff = reportYearDiff,
      sameFormType = toDouble(sameFormType),
      sameDisbursementCategory = toDouble(sameDisbursementCategory),
      sameEntityType = toDouble(sameEntityType)
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
    left_side.vendor.edge.src_id.equals(right_side.vendor.edge.src_id)
  }

  @deprecated
  private lazy val sameZip: Boolean = {
    same(_.address.zip_code)
  }

  private lazy val sameCity: Boolean = {
    same(_.address.city)
  }

  private lazy val sameState: Boolean = {
    same(_.address.state)
  }

  @deprecated
  private lazy val sameReportType: Boolean = {
    same(_.edge.report_year)
  }

  private lazy val sameFormType: Boolean = {
    same(_.edge.form_type)
  }

  private lazy val sameDisbursementCategory: Boolean = {
    same(_.edge.disbursement_category)
  }

  private lazy val sameEntityType: Boolean = {
    same(_.edge.entity_type)
  }

  private lazy val reportYearDiff: Double = {
    val left: Option[Long] = {
      left_side.vendor.edge.report_year
    }
    val right: Option[Long] = {
      right_side.vendor.edge.report_year
    }
    (left, right) match {
      case (Some(l), Some(r)) =>
        math.abs(l - r).toDouble
      case _ =>
        0.0d
    }
  }

  @deprecated
  private lazy val amountPaidDiffRatio: Double = {
    val left: Option[Double] = {
      left_side.vendor.edge.transaction_amount
    }
    val right: Option[Double] = {
      right_side.vendor.edge.transaction_amount
    }
    (left, right) match {
      case (Some(l), Some(r)) =>
        val max = {
          Seq(l, r).max
        }
        val diff = {
          math.abs(l - r)
        }
        if (max > 0.0) {
          diff / max
        } else {
          0.0
        }
      case _ =>
        0.0
    }
  }

  private def toDouble(bool: Boolean): Double = {
    bool.compare(false)
  }

  private def same[A](f: IdResVendor => Option[A]): Boolean = {
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
