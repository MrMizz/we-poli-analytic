package in.tap.we.poli.analytic.jobs.graph.vertices

import in.tap.base.spark.jobs.composite.TwoInOneOutJob
import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.graph.vertices.CommitteesVertexJob.CommitteeVertex
import in.tap.we.poli.analytic.jobs.graph.vertices.VendorsVertexJob.VendorVertex
import in.tap.we.poli.analytic.jobs.graph.vertices.VerticesUnionJob.AgnosticVertex
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.Address
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class VerticesUnionJob(val inArgs: TwoInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[CommitteeVertex],
  val readTypeTagB: universe.TypeTag[VendorVertex],
  val writeTypeTagA: universe.TypeTag[AgnosticVertex]
) extends TwoInOneOutJob[CommitteeVertex, VendorVertex, AgnosticVertex](inArgs, outArgs) {

  override def transform(input: (Dataset[CommitteeVertex], Dataset[VendorVertex])): Dataset[AgnosticVertex] = {
    val (committeeVertices: Dataset[CommitteeVertex], vendorVertices: Dataset[VendorVertex]) = input
    committeeVertices.map(AgnosticVertex.fromCommitteeVertex).union(vendorVertices.map(AgnosticVertex.fromVendorVertex))
  }

}

object VerticesUnionJob {

  final case class AgnosticVertex(
    uid: VertexId,
    name: String,
    alternate_names: Set[String],
    address: Address,
    alternate_addresses: Set[Address],
    is_committee: Boolean
  )

  object AgnosticVertex {

    def fromCommitteeVertex(committeeVertex: CommitteeVertex): AgnosticVertex = {
      AgnosticVertex(
        uid = committeeVertex.uid,
        name = committeeVertex.name,
        alternate_names = committeeVertex.committee_names.filterNot(_.equals(committeeVertex.name)),
        address = committeeVertex.address,
        alternate_addresses = committeeVertex.addresses.filterNot(_.equals(committeeVertex.address)),
        is_committee = true
      )
    }

    def fromVendorVertex(vendorVertex: VendorVertex): AgnosticVertex = {
      AgnosticVertex(
        uid = vendorVertex.uid,
        name = vendorVertex.name,
        alternate_names = vendorVertex.names.filterNot(_.equals(vendorVertex.name)),
        address = vendorVertex.address,
        alternate_addresses = vendorVertex.addresses.filterNot(_.equals(vendorVertex.address)),
        is_committee = false
      )
    }

  }

}
