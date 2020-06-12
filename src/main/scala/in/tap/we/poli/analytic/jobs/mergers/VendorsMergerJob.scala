package in.tap.we.poli.analytic.jobs.mergers

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob._
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformationJob.Vendor
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class VendorsMergerJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[Vendor],
  val writeTypeTagA: universe.TypeTag[UniqueVendor]
) extends OneInOneOutJob[Vendor, UniqueVendor](inArgs, outArgs) {

  override def transform(input: Dataset[Vendor]): Dataset[UniqueVendor] = {
    import spark.implicits._
    input
      .flatMap(UniqueVendor.fromVendorWithHash)
      .rdd
      .reduceByKey(UniqueVendor.reduce)
      .map {
        case (_, uniqueVendor: UniqueVendor) =>
          uniqueVendor
      }
      .toDS
  }

}

object VendorsMergerJob {

  final case class UniqueVendor(
    name: Option[String],
    names: Seq[String],
    city: Option[String],
    state: Option[String],
    zip_code: Option[String],
    sub_ids: Seq[Long],
    num_merged: Int
  )

  object UniqueVendor {

    def buildHash(vendor: Vendor): Option[String] = {
      for {
        name <- vendor.name.map(_.toLowerCase)
        city <- vendor.city.map(_.toLowerCase)
        state <- vendor.state.map(_.toLowerCase)
      } yield {
        s"${name}_${city}_$state"
      }
    }

    def fromVendor(vendor: Vendor): UniqueVendor = {
      UniqueVendor(
        name = vendor.name,
        names = vendor.name.toSeq,
        city = vendor.city,
        state = vendor.state,
        zip_code = vendor.zip_code,
        sub_ids = Seq(vendor.sub_id),
        num_merged = 1
      )
    }

    def fromVendorWithHash(vendor: Vendor): Option[(String, UniqueVendor)] = {
      for {
        hash <- UniqueVendor.buildHash(vendor)
      } yield {
        hash -> fromVendor(vendor)
      }
    }

    def reduce(left: UniqueVendor, right: UniqueVendor): UniqueVendor = {
      UniqueVendor(
        name = left.name,
        names = (left.names ++ right.names).distinct,
        city = left.city,
        state = left.state,
        zip_code = left.zip_code,
        sub_ids = left.sub_ids ++ right.sub_ids,
        num_merged = left.num_merged + right.num_merged
      )
    }

  }

}
