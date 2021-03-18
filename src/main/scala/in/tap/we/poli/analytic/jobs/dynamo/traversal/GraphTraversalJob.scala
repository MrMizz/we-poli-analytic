package in.tap.we.poli.analytic.jobs.dynamo.traversal

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.dynamo.traversal.GraphTraversalJob.GraphTraversal.TraversalWithCount
import in.tap.we.poli.analytic.jobs.dynamo.traversal.GraphTraversalJob.GraphTraversal
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.{AggregateExpenditureEdge, Analytics}
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

abstract class GraphTraversalJob(
  val inArgs: OneInArgs,
  val outArgs: OneOutArgs,
  val sortedBy: TraversalWithCount => TraversalWithCount
)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[AggregateExpenditureEdge],
  val writeTypeTagA: universe.TypeTag[GraphTraversal]
) extends OneInOneOutJob[AggregateExpenditureEdge, GraphTraversal](inArgs, outArgs) {

  override def transform(input: Dataset[AggregateExpenditureEdge]): Dataset[GraphTraversal] = {
    import spark.implicits._
    val sortedByBroadcast = {
      spark.sparkContext.broadcast(sortedBy)
    }
    input
      .flatMap(GraphTraversal.apply)
      .rdd
      .reduceByKey(GraphTraversal.reduce)
      .flatMap {
        case (vertexId: VertexId, traversalWithCount: TraversalWithCount) =>
          val sorted = {
            sortedByBroadcast.value(traversalWithCount)
          }
          GraphTraversal.paginate(vertexId, sorted)
      }
      .toDS
  }

}

object GraphTraversalJob {

  /** Max DynamoDB.BatchGetItem return size. */
  val PAGE_SIZE: Int = {
    100
  }

  /**
   * DynamoDB Graph Traversal.
   * We need a Primary Key, which we call [[vertex_id]].
   * We're enforcing Pagination for traversals, to avoid writing arrays
   * too large to DynamoDb.
   * We'll keep track of page count in an adjacent table.
   *
   * @param vertex_id          either a src_id or dst_id
   * @param page_num           index of page
   * @param related_vertex_ids traversal
   */
  final case class GraphTraversal(
    vertex_id: VertexId,
    page_num: Long,
    related_vertex_ids: Seq[VertexId]
  )

  object GraphTraversal {

    type TraversalWithCount = (Seq[(VertexId, Analytics)], Long)

    def apply(
      edge: AggregateExpenditureEdge
    ): Seq[(VertexId, TraversalWithCount)] = {
      Seq(
        edge.src_id -> (Seq(edge.dst_id -> edge.analytics) -> 1),
        edge.dst_id -> (Seq(edge.src_id -> edge.analytics) -> 1)
      )
    }

    def reduce(left: TraversalWithCount, right: TraversalWithCount): TraversalWithCount = {
      (left._1 ++ right._1, left._2 + right._2)
    }

    /**
     * Paginate traversal by [[PAGE_SIZE]].
     *
     * @param vertexId either a src_id or dst_id
     * @param sortedTraversalWithCount sorted pages of related vertex ids
     */
    def paginate(
      vertexId: VertexId,
      sortedTraversalWithCount: TraversalWithCount
    ): Seq[GraphTraversal] = {
      val numPages: VertexId = {
        sortedTraversalWithCount._2 % PAGE_SIZE match {
          case 0 => sortedTraversalWithCount._2 / PAGE_SIZE
          case _ => (sortedTraversalWithCount._2 / PAGE_SIZE) + 1
        }
      }
      val vertexIds: Seq[VertexId] = {
        sortedTraversalWithCount._1.map(_._1)
      }
      (1 to numPages.toInt by 1).map { pageNum: Int =>
        val page: Seq[VertexId] = {
          val from = (pageNum - 1) * PAGE_SIZE
          val until = pageNum * PAGE_SIZE
          vertexIds.slice(from, until)
        }
        GraphTraversal(
          vertex_id = vertexId,
          page_num = pageNum.toLong,
          related_vertex_ids = page
        )
      }
    }

  }

}
