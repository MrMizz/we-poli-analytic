package in.tap.we.poli.analytic.jobs.dynamo.traversal.n1

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.dynamo.traversal.n1.GraphTraversalJob.GraphTraversal
import in.tap.we.poli.analytic.jobs.dynamo.traversal.n1.GraphTraversalPageCountJob.GraphTraversalPageCount
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class GraphTraversalPageCountJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[GraphTraversal],
  val writeTypeTagA: universe.TypeTag[GraphTraversalPageCount]
) extends OneInOneOutJob[GraphTraversal, GraphTraversalPageCount](inArgs, outArgs) {

  override def transform(input: Dataset[GraphTraversal]): Dataset[GraphTraversalPageCount] = {
    import spark.implicits._
    input
      .map(GraphTraversalPageCount.apply)
      .rdd
      .reduceByKey(GraphTraversalPageCount.reduce)
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

object GraphTraversalPageCountJob {

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

  object GraphTraversalPageCount {

    def apply(graphTraversal: GraphTraversal): (VertexId, Long) = {
      graphTraversal.vertex_id -> graphTraversal.page_num
    }

    def reduce(left: Long, right: Long): Long = {
      Seq(left, right).max
    }

  }

}
