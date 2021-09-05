package in.tap.we.poli.analytic.jobs.dynamo.traversal

import in.tap.we.poli.analytic.jobs.dynamo.traversal.nx.InitJob.DstId
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.Analytics
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

  def paginate(
    sortBy: Analytics => Option[Double],
    srcIds: String,
    unsorted: Seq[DstId],
    count: Long
  ): Seq[Traversal] = {
    val sorted: List[VertexId] = {
      sort(sortBy, unsorted)
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

  def sort(sortBy: Analytics => Option[Double], unsorted: Seq[DstId]): List[VertexId] = {
    unsorted
      .sortBy { dstId: DstId =>
        sortBy(dstId.analytics)
      }
      .map(_.dst_id)
      .reverse
      .toList
  }

}
