package in.tap.we.poli.analytic.jobs.dynamo

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.dynamo.GraphTraversalJob.GraphTraversal
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.AggregateExpenditureEdge
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class GraphTraversalJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[AggregateExpenditureEdge],
  val writeTypeTagA: universe.TypeTag[GraphTraversal]
) extends OneInOneOutJob[AggregateExpenditureEdge, GraphTraversal](inArgs, outArgs) {

  override def transform(input: Dataset[AggregateExpenditureEdge]): Dataset[GraphTraversal] = {
    import spark.implicits._
    input
      .flatMap(GraphTraversal.apply)
      .rdd
      .reduceByKey(_ ++ _)
      .map {
        case ((vertexId: VertexId, direction: String), relatedVertexIds: Seq[VertexId]) =>
          GraphTraversal(
            vertexId,
            direction,
            relatedVertexIds
          )
      }
      .toDS
  }

}

object GraphTraversalJob {

  /**
   * DynamoDB Graph Traversal.
   * We need a Primary Key, which we call [[vertex_id]].
   * This is either a src_id or a dst_id, depending on the [[direction]].
   *
   * @param vertex_id either a src_id or dst_id
   * @param direction direction of traversal
   * @param related_vertex_ids traversal
   */
  final case class GraphTraversal(
    vertex_id: VertexId,
    direction: String,
    related_vertex_ids: Seq[VertexId]
  )

  object GraphTraversal {

    def apply(aggregateExpenditureEdge: AggregateExpenditureEdge): Seq[((VertexId, String), Seq[VertexId])] = {
      Seq(
        aggregateExpenditureEdge.src_id -> Directions.OUT -> Seq(aggregateExpenditureEdge.dst_id),
        aggregateExpenditureEdge.dst_id -> Directions.IN -> Seq(aggregateExpenditureEdge.src_id)
      )
    }

    object Directions {

      val IN: String = {
        "in"
      }

      val OUT: String = {
        "out"
      }

    }

  }

}
