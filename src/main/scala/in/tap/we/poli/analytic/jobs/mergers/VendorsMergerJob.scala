package in.tap.we.poli.analytic.jobs.mergers

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob._
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.Vendor
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
    uid: Long,
    uids: Seq[Long],
    name: Option[String],
    names: Seq[String],
    city: Option[String],
    state: Option[String],
    zip_code: Option[String],
    num_merged: Int
  )

  object UniqueVendor {

    def fromVendor(vendor: Vendor): UniqueVendor = {
      UniqueVendor(
        uid = vendor.uid,
        uids = Seq(vendor.uid),
        name = vendor.name,
        names = vendor.name.toSeq,
        city = vendor.city,
        state = vendor.state,
        zip_code = vendor.zip_code,
        num_merged = 1
      )
    }

    def fromVendorWithHash(vendor: Vendor): Option[(String, UniqueVendor)] = {
      for {
        hash <- vendor.hash1
      } yield {
        hash -> fromVendor(vendor)
      }
    }

    def reduce(left: UniqueVendor, right: UniqueVendor): UniqueVendor = {
      UniqueVendor(
        uid = Seq(left.uid, right.uid).min,
        uids = left.uids ++ right.uids,
        name = left.name,
        names = (left.names ++ right.names).distinct,
        city = left.city,
        state = left.state,
        zip_code = left.zip_code,
        num_merged = left.num_merged + right.num_merged
      )
    }

  }

}
