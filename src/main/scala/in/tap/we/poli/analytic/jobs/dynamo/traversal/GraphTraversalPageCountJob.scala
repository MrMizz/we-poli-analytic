package in.tap.we.poli.analytic.jobs.dynamo.traversal

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.dynamo.traversal.GraphTraversalPageCountJob.GraphTraversalPageCount
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class GraphTraversalPageCountJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[Traversal],
  val writeTypeTagA: universe.TypeTag[GraphTraversalPageCount]
) extends OneInOneOutJob[Traversal, GraphTraversalPageCount](inArgs, outArgs) {

  override def transform(input: Dataset[Traversal]): Dataset[GraphTraversalPageCount] = {
    import spark.implicits._
    input
      .map(GraphTraversalPageCount.apply)
      .rdd
      .reduceByKey(GraphTraversalPageCount.reduce)
      .map {
        case (srcIds: String, pageCount: Long) =>
          GraphTraversalPageCount(
            src_ids = srcIds,
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
   * @param src_ids  1 or more Src Ids aggregated as AND statement
   * @param page_count total number of pages
   */
  final case class GraphTraversalPageCount(
    src_ids: String,
    page_count: Long
  )

  object GraphTraversalPageCount {

    def apply(traversal: Traversal): (String, Long) = {
      traversal.src_ids -> traversal.page_num
    }

    def reduce(left: Long, right: Long): Long = {
      Seq(left, right).max
    }

  }

}
