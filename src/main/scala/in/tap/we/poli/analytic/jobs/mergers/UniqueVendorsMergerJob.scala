package in.tap.we.poli.analytic.jobs.mergers

import in.tap.base.spark.jobs.composite.TwoInOneOutJob
import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.connectors.Connection
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

@deprecated
class UniqueVendorsMergerJob(val inArgs: TwoInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[UniqueVendor],
  val readTypeTagB: universe.TypeTag[Connection],
  val writeTypeTagA: universe.TypeTag[UniqueVendor]
) extends TwoInOneOutJob[UniqueVendor, Connection, UniqueVendor](inArgs, outArgs) {

  override def transform(input: (Dataset[UniqueVendor], Dataset[Connection])): Dataset[UniqueVendor] = {
    import spark.implicits._
    val (uniqueVendors, connector) = {
      input
    }
    uniqueVendors
      .map { uniqueVendor: UniqueVendor =>
        uniqueVendor.uid -> uniqueVendor
      }
      .rdd
      .join {
        connector.rdd
      }
      .map {
        case (_, (uniqueVendor: UniqueVendor, connectorId: VertexId)) =>
          connectorId -> uniqueVendor
      }
      .reduceByKey(VendorsMergerJob.UniqueVendor.reduce)
      .toDS()
      .map(_._2)(writeEncoderA)
  }

}
