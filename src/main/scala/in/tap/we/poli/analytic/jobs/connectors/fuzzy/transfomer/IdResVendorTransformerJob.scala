package in.tap.we.poli.analytic.jobs.connectors.fuzzy.transfomer

import in.tap.base.spark.jobs.composite.TwoInTwoOutJob
import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.TwoOutArgs
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class IdResVendorTransformerJob(val inArgs: TwoInArgs, val outArgs: TwoOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[VendorsTransformerJob.Vendor],
  val readTypeTagB: universe.TypeTag[VendorsMergerJob.UniqueVendor],
  val writeTypeTagA: universe.TypeTag[IdResVendorTransformerJob.Source.Vendor],
  val writeTypeTagB: universe.TypeTag[IdResVendorTransformerJob.Source.UniqueVendor]
) extends TwoInTwoOutJob[
      VendorsTransformerJob.Vendor,
      VendorsMergerJob.UniqueVendor,
      IdResVendorTransformerJob.Source.Vendor,
      IdResVendorTransformerJob.Source.UniqueVendor
    ](
      inArgs,
      outArgs
    ) {

  override def transform(
    input: (Dataset[VendorsTransformerJob.Vendor], Dataset[VendorsMergerJob.UniqueVendor])
  ): (Dataset[IdResVendorTransformerJob.Source.Vendor], Dataset[IdResVendorTransformerJob.Source.UniqueVendor]) = {
    val (vendors, uniqueVendors) = {
      input
    }
    vendors
      .map(
        IdResVendorTransformerJob.Source.apply
      )(writeEncoderA) -> uniqueVendors
      .map(
        IdResVendorTransformerJob.Source.apply
      )(writeEncoderB)
  }

}

object IdResVendorTransformerJob {

  final case class Model(
    uid: Long,
    names: Set[String],
    cities: Set[String],
    zip_codes: Set[String],
    states: Set[String],
    src_ids: Set[VertexId]
  )

  sealed trait Source {
    val model: Model
  }

  object Source {

    final case class Vendor(
      model: Model
    ) extends Source

    final case class UniqueVendor(
      model: Model
    ) extends Source

    def apply(vendor: VendorsTransformerJob.Vendor): Source.Vendor = {
      Source.Vendor(
        Model(
          uid = vendor.uid,
          names = Set(vendor.name),
          cities = vendor.address.city.toSet,
          zip_codes = vendor.address.zip_code.toSet,
          states = vendor.address.state.toSet,
          src_ids = Set(vendor.edge.src_id)
        )
      )
    }

    def apply(uniqueVendor: VendorsMergerJob.UniqueVendor): Source.UniqueVendor = {
      Source.UniqueVendor(
        Model(
          uid = uniqueVendor.uid,
          names = uniqueVendor.names,
          cities = uniqueVendor.addresses.flatMap(_.city),
          zip_codes = uniqueVendor.addresses.flatMap(_.zip_code),
          states = uniqueVendor.addresses.flatMap(_.state),
          src_ids = uniqueVendor.edges.map(_.src_id)
        )
      )
    }

  }

}
