package in.tap.we.poli.analytic.jobs.mergers

import in.tap.base.spark.jobs.composite.TwoInOneOutJob
import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob._
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.Vendor
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class VendorsMergerJob(val inArgs: TwoInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[Vendor],
  val readTypeTagB: universe.TypeTag[(VertexId, VertexId)],
  val writeTypeTagA: universe.TypeTag[UniqueVendor]
) extends TwoInOneOutJob[Vendor, (VertexId, VertexId), UniqueVendor](inArgs, outArgs) {

  override def transform(input: (Dataset[Vendor], Dataset[(VertexId, VertexId)])): Dataset[UniqueVendor] = {
    import spark.implicits._

    val (vendors: Dataset[Vendor], connector: Dataset[(VertexId, VertexId)]) = input

    vendors
      .map { vendor: Vendor =>
        vendor.uid -> UniqueVendor.fromVendor(vendor)
      }
      .rdd
      .join(connector.rdd)
      .map {
        case (_, (uniqueVendor: UniqueVendor, connection: VertexId)) =>
          connection -> uniqueVendor
      }
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
    names: Set[String],
    city: Option[String],
    state: Option[String],
    zip_code: Option[String],
    memos: Set[String],
    num_merged: BigInt
  )

  object UniqueVendor {

    def fromVendor(vendor: Vendor): UniqueVendor = {
      UniqueVendor(
        uid = vendor.uid,
        uids = Seq(vendor.uid),
        name = vendor.name,
        names = vendor.name.toSet,
        city = vendor.city,
        state = vendor.state,
        zip_code = vendor.zip_code,
        memos = Set(vendor.memo).flatten.map(_.toLowerCase),
        num_merged = 1
      )
    }

    def reduce(left: UniqueVendor, right: UniqueVendor): UniqueVendor = {
      UniqueVendor(
        uid = Seq(left.uid, right.uid).min,
        uids = left.uids ++ right.uids,
        name = left.name,
        names = left.names ++ right.names,
        city = left.city,
        state = left.state,
        zip_code = left.zip_code,
        memos = left.memos ++ right.memos,
        num_merged = left.num_merged + right.num_merged
      )
    }

  }

}
