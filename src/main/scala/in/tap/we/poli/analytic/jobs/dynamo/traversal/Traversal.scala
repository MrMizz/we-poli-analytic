package in.tap.we.poli.analytic.jobs.dynamo.traversal

import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.{AggregateExpenditureEdge, Analytics}
import org.apache.spark.graphx.VertexId

/**
 * DynamoDB Graph Traversal with Pagination.
 *
 * @param src_ids 1 or more Src Ids aggregated as AND statement
 * @param page_num index of page
 * @param dst_ids traversal
 */
final case class Traversal(
  src_ids: String,
  page_num: Long,
  dst_ids: Seq[VertexId]
)

object Traversal {

  /** Max DynamoDB.BatchGetItem return size. */
  private val PAGE_SIZE: Int = {
    100
  }

  type TraversalWithCount = (Seq[(VertexId, Analytics)], Long)

  def apply(edge: AggregateExpenditureEdge): Seq[(VertexId, Seq[(VertexId, Analytics)])] = {
    Seq(
      edge.src_id -> Seq(edge.dst_id -> edge.analytics),
      edge.dst_id -> Seq(edge.src_id -> edge.analytics)
    )
  }

  def reduce(left: TraversalWithCount, right: TraversalWithCount): TraversalWithCount = {
    (left._1 ++ right._1, left._2 + right._2)
  }

  def paginate(
    sortBy: Analytics => Option[Double],
    srcIds: String,
    unsorted: Seq[(VertexId, Analytics)],
    count: Long
  ): Seq[Traversal] = {
    val sorted: List[VertexId] = {
      unsorted
        .sortBy {
          case (_, analytics) =>
            sortBy(analytics)
        }
        .map(_._1)
        .reverse
        .toList
    }
    paginate(srcIds, sorted, count)
  }

  def paginate(srcIds: String, sorted: Seq[VertexId], count: Long): Seq[Traversal] = {
    val numPages: Long = {
      count % PAGE_SIZE match {
        case 0 => count / PAGE_SIZE
        case _ => (count / PAGE_SIZE) + 1
      }
    }
    (1 to numPages.toInt by 1).map { pageNum: Int =>
      val page: Seq[VertexId] = {
        val from = (pageNum - 1) * PAGE_SIZE
        val until = pageNum * PAGE_SIZE
        sorted.slice(from, until)
      }
      Traversal(
        src_ids = srcIds,
        page_num = pageNum.toLong,
        dst_ids = page
      )
    }
  }

}
