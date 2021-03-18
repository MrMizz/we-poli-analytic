package in.tap.we.poli.analytic.jobs.transformers

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.connectors
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.ExpenditureEdge
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob._
import in.tap.we.poli.models.OperatingExpenditures
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class VendorsTransformerJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[OperatingExpenditures],
  val writeTypeTagA: universe.TypeTag[Vendor]
) extends OneInOneOutJob[OperatingExpenditures, Vendor](inArgs, outArgs) {

  override def transform(input: Dataset[OperatingExpenditures]): Dataset[Vendor] = {
    input.flatMap(Vendor.fromOperatingExpenditures)
  }

}

object VendorsTransformerJob {

  // TODO: state -> string lookup
  final case class Address(
    street: Option[String],
    alternate_street: Option[String],
    city: Option[String],
    zip_code: Option[String],
    state: Option[String]
  )

  object Address {

    val empty: Address = {
      Address(
        None,
        None,
        None,
        None,
        None
      )
    }

  }

  trait VendorLike {
    val uid: Long
    val name: String
    val address: Address
  }

  final case class Vendor(
    uid: Long,
    name: String,
    address: Address,
    memo: Option[String],
    edge: ExpenditureEdge
  ) extends VendorLike {

    lazy val hash1: Option[String] = {
      for {
        city <- address.city.map(_.toLowerCase)
        state <- address.state.map(_.toLowerCase)
      } yield {
        s"${name.toLowerCase}_${city}_$state"
      }
    }

    lazy val hash2: Option[String] = {
      for {
        city <- address.city.map(_.toLowerCase)
        state <- address.state.map(_.toLowerCase)
      } yield {
        s"${cleanedName}_${city}_$state"
      }
    }

    lazy val hash3: Option[String] = {
      for {
        zip <- address.zip_code.map(_.toLowerCase.take(5))
      } yield {
        s"${cleanedName}_$zip"
      }
    }

    lazy val hashes: Seq[String] = {
      Seq(
        hash1,
        hash2,
        hash3
      ).flatten
    }

    private lazy val cleanedName: String = {
      connectors.cleanedName(name)
    }

  }

  object Vendor {

    def fromOperatingExpenditures(operatingExpenditures: OperatingExpenditures): Option[Vendor] = {
      for {
        sub_id <- operatingExpenditures.SUB_ID
        name <- operatingExpenditures.NAME
      } yield {
        Vendor(
          uid = sub_id,
          name = name.toLowerCase,
          address = Address(
            street = None,
            alternate_street = None,
            city = operatingExpenditures.CITY.map(_.toLowerCase),
            state = operatingExpenditures.STATE.map(_.toLowerCase),
            zip_code = operatingExpenditures.ZIP_CODE.map(_.take(5))
          ),
          memo = operatingExpenditures.PURPOSE.map(_.toLowerCase),
          edge = ExpenditureEdge.fromOperatingExpenditures(operatingExpenditures)
        )
      }
    }

  }

}
