package in.tap.we.poli.analytic.jobs.connectors.unify

import in.tap.base.spark.graph.ConnectedComponents
import in.tap.base.spark.jobs.composite.TwoInOneOutJob
import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.connectors.unify.ConnectorsUnifyJob.distinct
import in.tap.we.poli.analytic.jobs.connectors.{buildEdges, Connection}
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

@deprecated("UniqueVendorsMergerJob")
class ConnectorsUnifyJob(val inArgs: TwoInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[Connection],
  val readTypeTagB: universe.TypeTag[Connection],
  val writeTypeTagA: universe.TypeTag[Connection]
) extends TwoInOneOutJob[Connection, Connection, Connection](inArgs, outArgs) {

  override def transform(input: (Dataset[Connection], Dataset[Connection])): Dataset[Connection] = {
    import spark.implicits._
    val (connector1, connector2) = {
      input
    }
    val vertexUnion: Dataset[Connection] = {
      connector1.union(connector2)
    }
    val edges: RDD[Edge[Int]] = {
      vertexUnion
        .rdd
        .map(_.swap)
        .groupByKey
        .flatMap {
          case (_, iter: Iterable[VertexId]) =>
            buildEdges(iter.toList)
        }
    }
    val vertices: RDD[Connection] = {
      vertexUnion.rdd.reduceByKey(distinct)
    }
    ConnectedComponents
      .withVertexAttr(vertices, edges)
      .toDS
      .map {
        case (uid, connectedId, _) =>
          uid -> connectedId
      }(writeEncoderA)
  }

}

object ConnectorsUnifyJob {

  /**
   * Distinct [[VertexId]] for [[ConnectedComponents]].
   * Here, we are uninterested in the Vertex Property, and
   * only care for the UID [[VertexId]]. If the UID shows up
   * in both inputs, arbitrarily select one.
   *
   * @param left uid
   * @param right uid
   * @return distinct uid
   */
  def distinct(left: VertexId, right: VertexId): VertexId = {
    right
  }

}
