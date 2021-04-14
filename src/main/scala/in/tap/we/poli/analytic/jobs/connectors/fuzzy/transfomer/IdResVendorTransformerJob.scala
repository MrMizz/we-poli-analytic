package in.tap.we.poli.analytic.jobs.connectors.fuzzy.transfomer

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.transfomer.IdResVendorTransformerJob.IdResVendor
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.{Address, Vendor}
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class IdResVendorTransformerJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[Vendor],
  val writeTypeTagA: universe.TypeTag[IdResVendor]
) extends OneInOneOutJob[Vendor, IdResVendor](inArgs, outArgs) {

  override def transform(input: Dataset[Vendor]): Dataset[IdResVendor] = {
    input.map(IdResVendor.apply)
  }

}

object IdResVendorTransformerJob {

  final case class IdResVendor(
    uid: Long,
    name: String,
    address: Address,
    edge: IdResEdge
  )

  object IdResVendor {

    def apply(vendor: Vendor): IdResVendor = {
      IdResVendor(
        uid = vendor.uid,
        name = vendor.name,
        address = vendor.address,
        edge = IdResEdge(vendor)
      )
    }

  }

  final case class IdResEdge(
    src_id: VertexId,
    report_year: Option[Long],
    report_type: Option[String],
    form_type: Option[String],
    transaction_amount: Option[Double],
    disbursement_category: Option[String],
    entity_type: Option[String]
  )

  object IdResEdge {

    def apply(vendor: Vendor): IdResEdge = {
      IdResEdge(
        src_id = vendor.edge.src_id,
        report_year = vendor.edge.report_year,
        report_type = vendor.edge.report_type,
        form_type = vendor.edge.form_type,
        transaction_amount = vendor.edge.transaction_amount,
        disbursement_category = vendor.edge.disbursement_category,
        entity_type = vendor.edge.entity_type
      )
    }

  }

}
