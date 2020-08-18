package in.tap.we.poli.analytic.jobs.dynamo

import in.tap.base.spark.jobs.composite.TwoInOneOutJob
import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.dynamo.VertexNameAutoCompleteJob.VertexNameAutoComplete
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.AggregateExpenditureEdge
import in.tap.we.poli.analytic.jobs.graph.vertices.VerticesUnionJob.AgnosticVertex
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class VertexNameAutoCompleteJob(val inArgs: TwoInArgs, val outArgs: OneOutArgs, val MAX_RESPONSE_SIZE: Int)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[AgnosticVertex],
  val readTypeTagB: universe.TypeTag[AggregateExpenditureEdge],
  val writeTypeTagA: universe.TypeTag[VertexNameAutoComplete]
) extends TwoInOneOutJob[AgnosticVertex, AggregateExpenditureEdge, VertexNameAutoComplete](inArgs, outArgs) {

  override def transform(
    input: (Dataset[AgnosticVertex], Dataset[AggregateExpenditureEdge])
  ): Dataset[VertexNameAutoComplete] = {
    val BC_MAX_RESPONSE_SIZE: Broadcast[Int] = {
      spark.sparkContext.broadcast(MAX_RESPONSE_SIZE)
    }
    val (vertices: Dataset[AgnosticVertex], edges: Dataset[AggregateExpenditureEdge]) = {
      input
    }
    import spark.implicits._
    import VertexNameAutoCompleteJob.VertexNameAutoComplete._
    vertices
      .flatMap(fromVertex)
      .rdd
      .join(
        edges
          .flatMap(fromEdge)
          .rdd
          .reduceByKey(_ + _)
      )
      .map {
        case (_, ((vertex: AgnosticVertex, prefix: String), rank: BigInt)) =>
          prefix -> Set(vertex -> rank)
      }
      .reduceByKey(reduce(BC_MAX_RESPONSE_SIZE.value))
      .map {
        case (prefix: String, verticesWithRank: Set[(AgnosticVertex, BigInt)]) =>
          VertexNameAutoComplete(
            prefix = prefix,
            prefix_size = prefix.length,
            vertices = verticesWithRank.map(_._1)
          )
      }
      .toDS
  }

}

object VertexNameAutoCompleteJob {

  /**
   * Auto Completing Name Search,
   * Sits in DynamoDB, serving vertex ids that belong
   * to entities with a name containing requested prefix.
   *
   * @param prefix requested
   * @param prefix_size of req.
   * @param vertices containing req. prefix
   */
  final case class VertexNameAutoComplete(
    prefix: String,
    prefix_size: BigInt,
    vertices: Set[AgnosticVertex]
  )

  object VertexNameAutoComplete {

    private val PREFIX_RANGE: Seq[Int] = {
      3 to 50 by 1
    }

    type VertexIdWithDataAndPrefix = (VertexId, (AgnosticVertex, String))

    def fromVertex(vertex: AgnosticVertex): Seq[VertexIdWithDataAndPrefix] = {
      buildPrefixes(vertex.name).map { prefix: String =>
        vertex.uid -> (vertex, prefix)
      }
    }

    type VertexIdWithRank = (VertexId, BigInt)

    def fromEdge(edge: AggregateExpenditureEdge): Seq[VertexIdWithRank] = {
      Seq(edge.src_id -> edge.num_edges, edge.dst_id -> edge.num_edges)
    }

    type VertexWithRank = (AgnosticVertex, BigInt)

    def reduce(
      MAX_RESPONSE_SIZE: Int
    )(left: Set[VertexWithRank], right: Set[VertexWithRank]): Set[VertexWithRank] = {
      (left ++ right).take(MAX_RESPONSE_SIZE)
    }

    def buildPrefixes(name: String): Seq[String] = {
      def func(token: String): Seq[String] = {
        val tokenSize: Int = token.length
        PREFIX_RANGE.filter(_ <= tokenSize).map { n: Int =>
          token.take(n)
        }
      }
      val lowerName: String = name.toLowerCase
      (lowerName.split(" ") :+ lowerName.replace(" ", "")).flatMap(func).distinct
    }

  }

}
