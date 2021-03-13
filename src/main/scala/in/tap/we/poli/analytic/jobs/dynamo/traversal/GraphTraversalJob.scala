package in.tap.we.poli.analytic.jobs.dynamo.traversal

import in.tap.base.spark.jobs.composite.OneInTwoOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.TwoOutArgs
import in.tap.we.poli.analytic.jobs.dynamo.traversal.GraphTraversalJob.GraphTraversal.RelatedVertexIdsWithCount
import in.tap.we.poli.analytic.jobs.dynamo.traversal.GraphTraversalJob.{GraphTraversal, GraphTraversalPageCount}
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.AggregateExpenditureEdge
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class GraphTraversalJob(val inArgs: OneInArgs, val outArgs: TwoOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[AggregateExpenditureEdge],
  val writeTypeTagA: universe.TypeTag[GraphTraversal],
  val writeTypeTagB: universe.TypeTag[GraphTraversalPageCount]
) extends OneInTwoOutJob[AggregateExpenditureEdge, GraphTraversal, GraphTraversalPageCount](inArgs, outArgs) {

  override def transform(
    input: Dataset[AggregateExpenditureEdge]
  ): (Dataset[GraphTraversal], Dataset[GraphTraversalPageCount]) = {
    import spark.implicits._
    val tuple: RDD[(GraphTraversal, (VertexId, VertexId))] = {
      input
        .flatMap(GraphTraversal.apply)
        .rdd
        .reduceByKey(GraphTraversal.reduce)
        .flatMap {
          case (vertexId: VertexId, relatedVertexIdsWithCount: RelatedVertexIdsWithCount) =>
            GraphTraversal.paginate(vertexId, relatedVertexIdsWithCount)
        }
    }.cache()
    tuple.map(_._1).toDS -> tuple
      .map(_._2)
      .reduceByKey(_ + _)
      .map {
        case (vertexId: VertexId, pageCount: Long) =>
          GraphTraversalPageCount(
            vertex_id = vertexId,
            page_count = pageCount
          )
      }
      .toDS
  }

}

object GraphTraversalJob {

  /** Max DynamoDB BatchGetItem return size. */
  val PAGE_SIZE: Int = {
    100
  }

  /**
   * Traversal Page Count Lookup.
   *
   * @param vertex_id  either a src_id or dst_id
   * @param page_count total number of pages
   */
  final case class GraphTraversalPageCount(
    vertex_id: VertexId,
    page_count: Long
  )

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

    type RelatedVertexIdsWithCount = (Seq[VertexId], Long)

    def apply(
      aggregateExpenditureEdge: AggregateExpenditureEdge
    ): Seq[(VertexId, RelatedVertexIdsWithCount)] = {
      Seq(
        aggregateExpenditureEdge.src_id -> (Seq(aggregateExpenditureEdge.dst_id) -> 1),
        aggregateExpenditureEdge.dst_id -> (Seq(aggregateExpenditureEdge.src_id) -> 1)
      )
    }

    def reduce(left: RelatedVertexIdsWithCount, right: RelatedVertexIdsWithCount): RelatedVertexIdsWithCount = {
      (left._1 ++ right._1, left._2 + right._2)
    }

    /**
     * Paginate traversal by [[PAGE_SIZE]].
     *
     * @param vertexId either a src_id or dst_id
     * @param relatedVertexIdsWithCount pages of related vertex ids
     */
    def paginate(
      vertexId: VertexId,
      relatedVertexIdsWithCount: RelatedVertexIdsWithCount
    ): Seq[(GraphTraversal, (VertexId, Long))] = {
      val numPages: VertexId = {
        relatedVertexIdsWithCount._2 % PAGE_SIZE match {
          case 0 => relatedVertexIdsWithCount._2 / PAGE_SIZE
          case _ => (relatedVertexIdsWithCount._2 / PAGE_SIZE) + 1
        }
      }
      val vertexIdsIterator: Iterator[VertexId] = {
        relatedVertexIdsWithCount._1.toIterator
      }
      (1 to numPages.toInt by 1).map { pageNum: Int =>
        val page: Seq[VertexId] = vertexIdsIterator.take(PAGE_SIZE).toSeq
        GraphTraversal(
          vertex_id = vertexId,
          page_num = pageNum.toLong,
          related_vertex_ids = page
        ) -> (vertexId -> 1L)
      }
    }

  }

}
