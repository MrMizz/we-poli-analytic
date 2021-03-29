package in.tap.we.poli.analytic.jobs.connectors.fuzzy.transfomer

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.transfomer.IdResVendorTransformerJob.IdResVendor
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.{Address, Vendor}
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
    src_id: Long,
    name: String,
    address: Address
  )

  object IdResVendor {

    def apply(vendor: Vendor): IdResVendor = {
      IdResVendor(
        uid = vendor.uid,
        src_id = vendor.edge.src_id,
        name = vendor.name,
        address = vendor.address
      )
    }

  }

}
