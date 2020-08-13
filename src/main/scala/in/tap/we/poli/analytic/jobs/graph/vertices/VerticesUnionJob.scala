package in.tap.we.poli.analytic.jobs.graph.vertices

import in.tap.base.spark.jobs.composite.TwoInOneOutJob
import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.graph.vertices.CommitteesVertexJob.CommitteeVertex
import in.tap.we.poli.analytic.jobs.graph.vertices.VendorsVertexJob.VendorVertex
import in.tap.we.poli.analytic.jobs.graph.vertices.VerticesUnionJob.AgnosticVertex
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

// TODO: Valid Vertices Union Job, flatMap model name as non-option for Dynamo.
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
    streets: Set[String],
    cities: Set[String],
    states: Set[String],
    is_committee: Boolean
  )

  object AgnosticVertex {

    def fromCommitteeVertex(committeeVertex: CommitteeVertex): AgnosticVertex = {
      AgnosticVertex(
        uid = committeeVertex.uid,
        name = committeeVertex.name,
        streets = committeeVertex.streets,
        cities = committeeVertex.cities,
        states = committeeVertex.states,
        is_committee = true
      )
    }

    def fromVendorVertex(vendorVertex: VendorVertex): AgnosticVertex = {
      AgnosticVertex(
        uid = vendorVertex.uid,
        name = vendorVertex.name,
        streets = Set.empty[String],
        cities = vendorVertex.city.toSet,
        states = vendorVertex.state.toSet,
        is_committee = false
      )
    }

  }

}
