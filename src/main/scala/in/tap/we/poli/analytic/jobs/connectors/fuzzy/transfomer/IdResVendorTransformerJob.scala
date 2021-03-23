package in.tap.we.poli.analytic.jobs.connectors.fuzzy.transfomer

import in.tap.base.spark.jobs.composite.TwoInTwoOutJob
import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.TwoOutArgs
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.transfomer.IdResVendorTransformerJob.IdResVendor
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.Vendor
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class IdResVendorTransformerJob(val inArgs: TwoInArgs, val outArgs: TwoOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[Vendor],
  val readTypeTagB: universe.TypeTag[UniqueVendor],
  val writeTypeTagA: universe.TypeTag[IdResVendor],
  val writeTypeTagB: universe.TypeTag[IdResVendor]
) extends TwoInTwoOutJob[Vendor, UniqueVendor, IdResVendor, IdResVendor](inArgs, outArgs) {

  override def transform(
    input: (Dataset[Vendor], Dataset[UniqueVendor])
  ): (Dataset[IdResVendor], Dataset[IdResVendor]) = {
    val (vendors, uniqueVendors) = {
      input
    }
    vendors
      .map(
        IdResVendor.apply
      )(writeEncoderA) -> uniqueVendors
      .map(
        IdResVendor.apply
      )(writeEncoderB)
  }

}

object IdResVendorTransformerJob {

  final case class IdResVendor(
    uid: Long,
    names: Set[String],
    cities: Set[String],
    zip_codes: Set[String],
    states: Set[String],
    src_ids: Set[VertexId]
  )

  object IdResVendor {

    def apply(vendor: Vendor): IdResVendor = {
      IdResVendor(
        uid = vendor.uid,
        names = Set(vendor.name),
        cities = vendor.address.city.toSet,
        zip_codes = vendor.address.zip_code.toSet,
        states = vendor.address.state.toSet,
        src_ids = vendor.edges.map(_.src_id)
      )
    }

    def apply(uniqueVendor: UniqueVendor): IdResVendor = {
      IdResVendor(
        uid = uniqueVendor.uid,
        names = uniqueVendor.names,
        cities = uniqueVendor.addresses.flatMap(_.city),
        zip_codes = uniqueVendor.addresses.flatMap(_.zip_code),
        states = uniqueVendor.addresses.flatMap(_.state),
        src_ids = uniqueVendor.edges.map(_.src_id)
      )
    }

  }

}