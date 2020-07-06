package in.tap.we.poli.analytic.jobs.graph

import in.tap.base.spark.jobs.composite.CompositeJob
import in.tap.base.spark.jobs.in.dataset.TwoInJob
import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.TwoOutArgs
import in.tap.we.poli.analytic.jobs.graph.NeptuneJob.{NeptuneEdge, NeptuneVertex}
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.AggregateExpenditureEdge
import in.tap.we.poli.analytic.jobs.graph.vertices.VerticesUnionJob.AgnosticVertex
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.reflect.runtime.universe

/**
 * Bare Minimum Vertices & Edges for AWS Neptune.
 * An online database, we'll use Neptune for Apache Gremlin Queries.
 * The Property data will live in AWS DynamoDB, which we'll be fetched
 * with secondary queries. Why? Neptune doesn't support struct types.
 */
class NeptuneJob(val inArgs: TwoInArgs, val outArgs: TwoOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[AgnosticVertex],
  val readTypeTagB: universe.TypeTag[AggregateExpenditureEdge]
) extends CompositeJob(inArgs, outArgs)
    with TwoInJob[AgnosticVertex, AggregateExpenditureEdge] {

  override def execute(): Unit = {
    import spark.implicits._

    val (vertices: Dataset[AgnosticVertex], edges: Dataset[AggregateExpenditureEdge]) = read

    val neptuneVertices: Dataset[NeptuneVertex] = {
      vertices.map(NeptuneVertex.apply)
    }

    val neptuneEdges: Dataset[NeptuneEdge] = {
      edges
        .rdd
        .zipWithUniqueId()
        .cache()
        .map {
          case (edge: AggregateExpenditureEdge, uid: Long) =>
            NeptuneEdge(uid, edge)
        }
        .toDS
    }

    neptuneVertices
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(outArgs.out1.path)

    neptuneEdges
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(outArgs.out2.path)
  }

}

object NeptuneJob {

  final case class NeptuneVertex(
    `~id`: Long,
    `is_committee:Bool`: Boolean,
    `is_vendor:Bool`: Boolean
  )

  object NeptuneVertex {

    def apply(agnosticVertex: AgnosticVertex): NeptuneVertex = {
      new NeptuneVertex(
        `~id` = agnosticVertex.uid,
        `is_committee:Bool` = agnosticVertex.is_committee,
        `is_vendor:Bool` = agnosticVertex.is_vendor
      )
    }

  }

  final case class NeptuneEdge(
    `~id`: Long,
    `~from`: Long,
    `~to`: Long
  )

  object NeptuneEdge {

    def apply(rowId: Long, edge: AggregateExpenditureEdge): NeptuneEdge = {
      new NeptuneEdge(
        `~id` = rowId,
        `~from` = edge.src_id,
        `~to` = edge.dst_id
      )
    }

  }

}
