package in.tap.we.poli.analytic.jobs.connectors.unify

import in.tap.base.spark.jobs.composite.TwoInOneOutJob
import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.connectors.Connection
import in.tap.we.poli.analytic.jobs.connectors.unify.UniqueVendorConnectorFlattenJob.Family
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class UniqueVendorConnectorFlattenJob(val inArgs: TwoInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[UniqueVendor],
  val readTypeTagB: universe.TypeTag[Connection],
  val writeTypeTagA: universe.TypeTag[Connection]
) extends TwoInOneOutJob[UniqueVendor, Connection, Connection](inArgs, outArgs) {

  override def transform(input: (Dataset[UniqueVendor], Dataset[Connection])): Dataset[Connection] = {
    import spark.implicits._
    val (uniqueVendors, connector) = {
      input
    }
    uniqueVendors
      .flatMap(Family.apply)(readEncoderB)
      .rdd
      .join {
        connector.rdd
      }
      .toDS
      .map {
        case (_, (childId, connectorId)) =>
          (childId, connectorId)
      }(writeEncoderA)
  }

}

object UniqueVendorConnectorFlattenJob {

  /**
   * Unique Vendor Id (parent)
   * Vendor Id (child)
   */
  type Family = (VertexId, VertexId)

  object Family {

    def apply(uniqueVendor: UniqueVendor): Seq[Family] = {
      uniqueVendor.uids.map { childId: VertexId =>
        (uniqueVendor.uid, childId)
      }
    }

  }

}
