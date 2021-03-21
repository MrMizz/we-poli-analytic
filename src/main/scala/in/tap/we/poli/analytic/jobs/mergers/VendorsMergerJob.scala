package in.tap.we.poli.analytic.jobs.mergers

import in.tap.base.spark.jobs.composite.TwoInOneOutJob
import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.ExpenditureEdge
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob._
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.{Address, Vendor, VendorLike}
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
    val (vendors: Dataset[Vendor], connector: Dataset[(VertexId, VertexId)]) = {
      input
    }
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
    name: String,
    names: Set[String],
    address: Address,
    addresses: Set[Address],
    memos: Set[String],
    edges: Set[ExpenditureEdge],
    num_merged: BigInt
  ) extends VendorLike

  object UniqueVendor {

    def fromVendor(vendor: Vendor): UniqueVendor = {
      UniqueVendor(
        uid = vendor.uid,
        uids = Seq(vendor.uid),
        name = vendor.name,
        names = Set(vendor.name),
        address = vendor.address,
        addresses = Set(vendor.address),
        memos = Set(vendor.memo).flatten,
        edges = vendor.edges,
        num_merged = 1
      )
    }

    def reduce(left: UniqueVendor, right: UniqueVendor): UniqueVendor = {
      val names: Set[String] = {
        left.names ++ right.names
      }
      val addresses: Set[Address] = {
        left.addresses ++ right.addresses
      }
      UniqueVendor(
        uid = Seq(left.uid, right.uid).min,
        uids = left.uids ++ right.uids,
        name = getMostCommon(names.toSeq).getOrElse(left.name),
        names = names,
        address = getMostCommon(addresses.toSeq).getOrElse(left.address),
        addresses = addresses,
        memos = left.memos ++ right.memos,
        edges = left.edges ++ right.edges,
        num_merged = left.num_merged + right.num_merged
      )
    }

  }

}
