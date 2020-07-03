package in.tap.we.poli.analytic.jobs.graph.vertices

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.graph.vertices.VendorsVertexJob.VendorVertex
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class VendorsVertexJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[UniqueVendor],
  val writeTypeTagA: universe.TypeTag[VendorVertex]
) extends OneInOneOutJob[UniqueVendor, VendorVertex](inArgs, outArgs) {

  override def transform(input: Dataset[UniqueVendor]): Dataset[VendorVertex] = {
    input.map(VendorVertex.fromVendor)
  }

}

object VendorsVertexJob {

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

    def fromVendor(uniqueVendor: UniqueVendor): VendorVertex = {
      val hasBeenStaff: Option[Boolean] = {
        hasBeenStaffCheck(uniqueVendor.memos)
      }
      val hasBeenConsultant: Option[Boolean] = {
        hasBeenConsultantCheck(uniqueVendor.memos)
      }
      val hasBeenAffiliated = {
        hasBeenAffiliatedCheck(hasBeenStaff, hasBeenConsultant)
      }
      VendorVertex(
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
