package in.tap.we.poli.analytic.jobs.dynamo.traversal

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.dynamo.traversal.nx.NxTraversalBuilder
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.{AggregateExpenditureEdge, Analytics}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe

abstract class GraphTraversalJob[A <: NxTraversalBuilder](
  val inArgs: OneInArgs,
  val outArgs: OneOutArgs,
  val sortBy: Analytics => Option[Double],
  val builder: A
)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[AggregateExpenditureEdge],
  val writeTypeTagA: universe.TypeTag[Traversal],
  val builderClassTag: ClassTag[A]
) extends OneInOneOutJob[AggregateExpenditureEdge, Traversal](inArgs, outArgs) {

  override def transform(input: Dataset[AggregateExpenditureEdge]): Dataset[Traversal] = {
    import spark.implicits._
    val sortByBroadcast: Broadcast[Analytics => Option[Double]] = {
      spark.sparkContext.broadcast(sortBy)
    }
    val builderBroadcast: Broadcast[A] = {
      spark.sparkContext.broadcast(builder)
    }
    input
      .flatMap {
        Traversal.apply
      }
      .rdd
      .reduceByKey {
        _ ++ _
      }
      .flatMap {
        case (vertexId: VertexId, traversals: Seq[(VertexId, Analytics)]) =>
          builderBroadcast.value(vertexId, traversals)
      }
      .reduceByKey {
        Traversal.reduce
      }
      .flatMap {
        case (srcIds: String, (traversals: Seq[(VertexId, Analytics)], count: Long)) =>
          Traversal.paginate(
            sortByBroadcast.value,
            srcIds,
            traversals,
            count
          )
      }
      .toDS
  }

}
