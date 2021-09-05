package in.tap.we.poli.analytic.jobs.dynamo.traversal.n1

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.dynamo.traversal.nx.InitJob.DstId
import in.tap.we.poli.analytic.jobs.dynamo.traversal.nx.NxKey.N1Key
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

final class N1InitJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[(VertexId, DstId)],
  val writeTypeTagA: universe.TypeTag[(N1Key, DstId.WithCount)]
) extends OneInOneOutJob[(VertexId, DstId), (N1Key, DstId.WithCount)](inArgs, outArgs) {

  override def transform(input: Dataset[(VertexId, DstId)]): Dataset[(N1Key, DstId.WithCount)] = {
    import spark.implicits._
    N1InitJob.apply(input.rdd).toDS
  }

}

object N1InitJob {

  def apply(rdd: RDD[(VertexId, DstId)])(implicit spark: SparkSession): RDD[(N1Key, DstId.WithCount)] = {
    rdd
      .map {
        apply
      }
      .reduceByKey {
        DstId.WithCount.reduce
      }
  }

  private def apply(tup: (VertexId, DstId)): (N1Key, DstId.WithCount) = {
    (N1Key(tup._1), DstId.WithCount(Seq(tup._2), 1L))
  }

}
