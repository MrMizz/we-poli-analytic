package in.tap.we.poli.analytic.jobs.dynamo.traversal.n1

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.dynamo.traversal.Traversal
import in.tap.we.poli.analytic.jobs.dynamo.traversal.nx.InitJob.DstId
import in.tap.we.poli.analytic.jobs.dynamo.traversal.nx.NxKey.N1Key
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.Analytics
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class N1Job(val inArgs: OneInArgs, val outArgs: OneOutArgs, val sortBy: Analytics => Option[Double])(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[(N1Key, DstId.WithCount)],
  val writeTypeTagA: universe.TypeTag[Traversal]
) extends OneInOneOutJob[(N1Key, DstId.WithCount), Traversal](inArgs, outArgs) {

  override def transform(input: Dataset[(N1Key, DstId.WithCount)]): Dataset[Traversal] = {
    val sortByBroadcast = {
      spark.sparkContext.broadcast(sortBy)
    }
    input
      .flatMap {
        case (n1Key: N1Key, withCount: DstId.WithCount) =>
          Traversal.paginate(
            sortByBroadcast.value,
            n1Key.key,
            withCount.dst_ids,
            withCount.count
          )
      }
  }

}
