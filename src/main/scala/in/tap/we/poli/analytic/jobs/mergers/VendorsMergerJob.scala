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
    name_freq: Map[String, Long],
    address: Address,
    addresses: Set[Address],
    address_freq: Map[Address, Long],
    memos: Set[String],
    edges: Set[ExpenditureEdge],
    num_merged: Long
  ) extends VendorLike

  object UniqueVendor {

    def fromVendor(vendor: Vendor): UniqueVendor = {
      UniqueVendor(
        uid = vendor.uid,
        uids = Seq(vendor.uid),
        name = vendor.name,
        names = Set(vendor.name),
        name_freq = SetFreq.init(vendor.name),
        address = vendor.address,
        addresses = Set(vendor.address),
        address_freq = SetFreq.init(vendor.address),
        memos = Set(vendor.memo).flatten,
        edges = Set(vendor.edge),
        num_merged = 1
      )
    }

    def reduce(left: UniqueVendor, right: UniqueVendor): UniqueVendor = {
      val nameFreq: Map[String, Long] = {
        SetFreq.reduce(left.name_freq, right.name_freq)
      }
      val addressFreq: Map[Address, Long] = {
        SetFreq.reduce(left.address_freq, right.address_freq)
      }
      UniqueVendor(
        uid = Seq(left.uid, right.uid).min,
        uids = left.uids ++ right.uids,
        name = SetFreq.getMostCommon(nameFreq).getOrElse(left.name),
        names = left.names ++ right.names,
        name_freq = nameFreq,
        address = SetFreq.getMostCommon(addressFreq).getOrElse(left.address),
        addresses = left.addresses ++ right.addresses,
        address_freq = addressFreq,
        memos = left.memos ++ right.memos,
        edges = left.edges ++ right.edges,
        num_merged = left.num_merged + right.num_merged
      )
    }

  }

}
