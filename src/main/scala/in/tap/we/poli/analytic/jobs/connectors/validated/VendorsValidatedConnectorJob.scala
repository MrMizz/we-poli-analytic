package in.tap.we.poli.analytic.jobs.connectors.validated

import in.tap.base.spark.graph.ConnectedComponents
import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.connectors.Connection
import in.tap.we.poli.analytic.jobs.connectors.validated.VendorsValidatedConnectorJob.Validation
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class VendorsValidatedConnectorJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[Validation],
  val writeTypeTagA: universe.TypeTag[Connection]
) extends OneInOneOutJob[Validation, Connection](inArgs, outArgs) {

  override def transform(input: Dataset[Validation]): Dataset[Connection] = {
    import spark.implicits._
    val vertices: RDD[(VertexId, Long)] = {
      input
        .flatMap(Validation.toVertex)
        .rdd
        .reduceByKey {
          case (left, _) =>
            left
        }
    }
    val edges: RDD[Edge[VertexId]] = {
      input.map(Validation.toEdge).rdd
    }
    ConnectedComponents(
      vertices,
      edges
    ).toDS()
  }

}

object VendorsValidatedConnectorJob {

  final case class Validation(
    l: VertexId,
    r: VertexId
  )

  object Validation {

    def toVertex(validated: Validation): Seq[(VertexId, Long)] = {
      Seq(validated.l -> 1L, validated.r -> 1L)
    }

    def toEdge(validated: Validation): Edge[Long] = {
      Edge(srcId = validated.l, dstId = validated.r, attr = 1L)
    }

  }

}
