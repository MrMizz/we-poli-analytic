package in.tap.we.poli.analytic.jobs.transformers

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformationJob._
import in.tap.we.poli.models.OperatingExpenditures
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class VendorsTransformationJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[OperatingExpenditures],
  val writeTypeTagA: universe.TypeTag[Vendor]
) extends OneInOneOutJob[OperatingExpenditures, Vendor](inArgs, outArgs) {

  override def transform(input: Dataset[OperatingExpenditures]): Dataset[Vendor] = {
    input.flatMap(Vendor.fromOperatingExpenditures)
  }

}

object VendorsTransformationJob {

  final case class Vendor(
    sub_id: Long,
    name: Option[String],
    city: Option[String],
    state: Option[String],
    zip_code: Option[String]
  )

  object Vendor {

    def fromOperatingExpenditures(operatingExpenditures: OperatingExpenditures): Option[Vendor] = {
      for {
        sub_id <- operatingExpenditures.SUB_ID
      } yield {
        Vendor(
          sub_id = sub_id,
          name = operatingExpenditures.NAME,
          city = operatingExpenditures.CITY,
          state = operatingExpenditures.STATE,
          zip_code = operatingExpenditures.ZIP_CODE
        )
      }
    }

  }

}
