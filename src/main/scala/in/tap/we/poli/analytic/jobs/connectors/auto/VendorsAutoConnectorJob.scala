package in.tap.we.poli.analytic.jobs.connectors.auto

import in.tap.base.spark.graph.ConnectedComponents
import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.connectors.buildEdges
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.Vendor
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class VendorsAutoConnectorJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[Vendor],
  val writeTypeTagA: universe.TypeTag[(VertexId, VertexId)]
) extends OneInOneOutJob[Vendor, (VertexId, VertexId)](inArgs, outArgs) {

  override def transform(input: Dataset[Vendor]): Dataset[(VertexId, VertexId)] = {
    import spark.implicits._
    val vertices: RDD[(VertexId, String)] = {
      input.map(VendorsAutoConnectorJob.fromVendorToVertex).rdd
    }
    val edges: RDD[Edge[Int]] = {
      input
        .flatMap(VendorsAutoConnectorJob.fromVendorToEdges)
        .groupByKey { case (hash, _) => hash }
        .flatMapGroups {
          case (_, iter: Iterator[(String, VertexId)]) =>
            buildEdges(iter.toList.map(_._2))
        }
        .rdd
    }
    ConnectedComponents(
      vertices,
      edges
    ).toDS
  }

}

object VendorsAutoConnectorJob {

  def fromVendorToVertex(vendor: Vendor): (VertexId, String) = {
    vendor.uid -> vendor.name
  }

  def fromVendorToEdges(vendor: Vendor): Seq[(String, VertexId)] = {
    vendor.hashes.map { hash: String =>
      hash -> vendor.uid
    }
  }

}
