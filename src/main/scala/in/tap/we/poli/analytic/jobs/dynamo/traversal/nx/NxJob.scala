package in.tap.we.poli.analytic.jobs.dynamo.traversal.nx

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.dynamo.traversal.Traversal
import in.tap.we.poli.analytic.jobs.dynamo.traversal.nx.InitJob.DstId
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.Analytics
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class NxJob[A <: NxKey](val inArgs: OneInArgs, val outArgs: OneOutArgs, val sortBy: Analytics => Option[Double])(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[(A, DstId.WithCount)],
  val writeTypeTagA: universe.TypeTag[Traversal]
) extends OneInOneOutJob[(A, DstId.WithCount), Traversal](inArgs, outArgs) {

  override def transform(input: Dataset[(A, DstId.WithCount)]): Dataset[Traversal] = {
    val sortByBroadcast = {
      spark.sparkContext.broadcast(sortBy)
    }
    input
      .flatMap {
        case (nxKey: A, withCount: DstId.WithCount) =>
          Traversal.paginate(
            sortByBroadcast.value,
            nxKey.key,
            withCount.dst_ids,
            withCount.count
          )
      }
  }

}
