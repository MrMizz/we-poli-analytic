package in.tap.we.poli.analytic.jobs.connectors

import in.tap.base.spark.graph.ConnectedComponents
import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.Vendor
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class VendorsConnectorJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[Vendor],
  val writeTypeTagA: universe.TypeTag[(VertexId, VertexId)]
) extends OneInOneOutJob[Vendor, (VertexId, VertexId)](inArgs, outArgs) {

  override def transform(input: Dataset[Vendor]): Dataset[(VertexId, VertexId)] = {
    import spark.implicits._

    val vertices: RDD[(VertexId, Option[String])] = {
      input.map(VendorsConnectorJob.fromVendorToVertex).rdd
    }

    val edges: RDD[Edge[Int]] = {
      input
        .flatMap(VendorsConnectorJob.fromVendorToEdges)
        .groupByKey { case (hash, _) => hash }
        .flatMapGroups {
          case (_, iter: Iterator[(String, VertexId)]) =>
            VendorsConnectorJob.buildEdges(iter)
        }
        .rdd
    }

    ConnectedComponents(vertices, edges).toDS
  }

}

object VendorsConnectorJob {

  def fromVendorToVertex(vendor: Vendor): (VertexId, Option[String]) = {
    vendor.uid -> vendor.name
  }

  def fromVendorToEdges(vendor: Vendor): Seq[(String, VertexId)] = {
    vendor.hashes.map { hash: String =>
      hash -> vendor.uid
    }
  }

  def buildEdges(grouped: Iterator[(String, VertexId)]): Iterator[Edge[Int]] = {
    val head: VertexId = grouped.next()._2
    grouped.map {
      case (_, uid: VertexId) =>
        Edge(srcId = head, dstId = uid, attr = 1)
    }
  }

}
