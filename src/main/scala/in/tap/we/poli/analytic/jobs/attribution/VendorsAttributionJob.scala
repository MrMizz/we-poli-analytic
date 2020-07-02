package in.tap.we.poli.analytic.jobs.attribution

import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor
import org.apache.spark.graphx.VertexId

class VendorsAttributionJob(inArgs: OneInArgs, outArgs: OneOutArgs)

object VendorsAttributionJob {

  final case class VendorVertex(
    uid: VertexId,
    name: Option[String],
    city: Option[String],
    zip: Option[String],
    state: Option[String],
    has_been_affiliated: Option[Boolean],
    has_been_staff: Option[Boolean],
    has_been_consultant: Option[Boolean]
  )

  object VendorVertex {

    val STAFF_KEY_WORDS: Set[String] = {
      Set(
        "payroll",
        "salary",
        "salaries"
      )
    }

    val CONSULTANT_KEY_WORD: String = {
      "consul"
    }

    def fromVendor(uniqueVendor: UniqueVendor): (VertexId, VendorVertex) = {
      val hasBeenStaff: Option[Boolean] = {
        hasBeenStaffCheck(uniqueVendor.memos)
      }
      val hasBeenConsultant: Option[Boolean] = {
        hasBeenConsultantCheck(uniqueVendor.memos)
      }
      val hasBeenAffiliated = {
        hasBeenAffiliatedCheck(hasBeenStaff, hasBeenConsultant)
      }
      uniqueVendor.uid -> VendorVertex(
        uid = uniqueVendor.uid,
        name = uniqueVendor.name,
        city = uniqueVendor.city,
        zip = uniqueVendor.zip_code,
        state = uniqueVendor.state,
        has_been_affiliated = hasBeenAffiliated,
        has_been_staff = hasBeenStaff,
        has_been_consultant = hasBeenConsultant
      )
    }

    private def hasBeenStaffCheck(memos: Set[String]): Option[Boolean] = {
      if (STAFF_KEY_WORDS.intersect(memos).nonEmpty) {
        Some(true)
      } else {
        None
      }
    }

    private def hasBeenConsultantCheck(memos: Set[String]): Option[Boolean] = {
      if (memos.exists(_.contains(CONSULTANT_KEY_WORD))) {
        Some(true)
      } else {
        None
      }
    }

    private def hasBeenAffiliatedCheck(
      hasBeenStaff: Option[Boolean],
      hasBeenConsultant: Option[Boolean]
    ): Option[Boolean] = {
      (hasBeenStaff, hasBeenConsultant) match {
        case (Some(true), _) => Some(true)
        case (_, Some(true)) => Some(true)
        case _               => None
      }
    }

  }

}
