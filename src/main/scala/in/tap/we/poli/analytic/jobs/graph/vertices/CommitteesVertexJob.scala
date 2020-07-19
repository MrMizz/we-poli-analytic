package in.tap.we.poli.analytic.jobs.graph.vertices

import in.tap.base.spark.jobs.composite.TwoInOneOutJob
import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.AggregateExpenditureEdge
import in.tap.we.poli.analytic.jobs.graph.vertices.CommitteesVertexJob.CommitteeVertex
import in.tap.we.poli.models.Committee
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class CommitteesVertexJob(val inArgs: TwoInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[Committee],
  val readTypeTagB: universe.TypeTag[AggregateExpenditureEdge],
  val writeTypeTagA: universe.TypeTag[CommitteeVertex]
) extends TwoInOneOutJob[Committee, AggregateExpenditureEdge, CommitteeVertex](inArgs, outArgs) {

  override def transform(input: (Dataset[Committee], Dataset[AggregateExpenditureEdge])): Dataset[CommitteeVertex] = {
    import spark.implicits._
    val (committees: Dataset[Committee], edges: Dataset[AggregateExpenditureEdge]) = {
      input
    }
    val vertices: RDD[(VertexId, (CommitteeVertex, Boolean))] = {
      committees
        .map(CommitteeVertex.fromCommittee)
        .rdd
        .reduceByKey(CommitteeVertex.reduce)
        .map {
          case (vertexId: VertexId, vertex: CommitteeVertex) =>
            vertexId -> (vertex, true)
        }
    }
    val srcVertices: RDD[(VertexId, (CommitteeVertex, Boolean))] = {
      edges
        .map(_.src_id)
        .distinct
        .map { vertexId: VertexId =>
          vertexId -> (CommitteesVertexJob.emptyCommitteeVertex(vertexId), false)
        }
        .rdd
    }
    vertices
      .union(srcVertices)
      .reduceByKey { (left: (CommitteeVertex, Boolean), right: (CommitteeVertex, Boolean)) =>
        Seq(left, right).maxBy(_._2)
      }
      .map {
        case (_, (vertex: CommitteeVertex, _)) =>
          vertex
      }
      .toDS
  }

}

object CommitteesVertexJob {

  /**
   * There are a few cases where vendors appear in Operating Expenditures,
   * while not appearing in the Master Committee File.
   * This is a big problem for populating a graph DB (Neptune),
   * so we'll need to ensure we have at least an Empty Vertex
   * for these cases.
   *
   * @param uid missing committee id
   * @return empty vertex
   */
  def emptyCommitteeVertex(uid: VertexId): CommitteeVertex = {
    CommitteeVertex(
      uid = uid,
      committee_names = Set.empty[String],
      treasures_names = Set.empty[String],
      streets = Set.empty[String],
      cities = Set.empty[String],
      states = Set.empty[String],
      zip_codes = Set.empty[String],
      committee_designations = Set.empty[String],
      committee_types = Set.empty[String],
      committee_party_affiliations = Set.empty[String],
      interest_group_categories = Set.empty[String],
      connected_organization_names = Set.empty[String],
      candidate_ids = Set.empty[String]
    )
  }

  final case class CommitteeVertex(
    uid: VertexId,
    committee_names: Set[String],
    treasures_names: Set[String],
    streets: Set[String],
    cities: Set[String],
    states: Set[String],
    zip_codes: Set[String],
    committee_designations: Set[String],
    committee_types: Set[String],
    committee_party_affiliations: Set[String],
    interest_group_categories: Set[String],
    connected_organization_names: Set[String],
    candidate_ids: Set[String]
  )

  object CommitteeVertex {

    def fromStringToLongUID(committeeUID: String): VertexId = {
      committeeUID.replace("C", "1").toLong
    }

    def fromCommittee(committee: Committee): (VertexId, CommitteeVertex) = {
      val vertexId: VertexId = {
        fromStringToLongUID(committee.CMTE_ID)
      }
      vertexId -> CommitteeVertex(
        uid = vertexId,
        committee_names = committee.CMTE_NM.toSet,
        treasures_names = committee.TRES_NM.toSet,
        streets = committee.CMTE_ST1.toSet ++ committee.CMTE_ST2,
        cities = committee.CMTE_CITY.toSet,
        states = committee.CMTE_ST.toSet,
        zip_codes = committee.CMTE_ZIP.toSet,
        committee_designations = committee.CMTE_DSGN.toSet,
        committee_types = committee.CMTE_TP.toSet,
        committee_party_affiliations = committee.CMTE_PTY_AFFILIATION.toSet,
        interest_group_categories = committee.ORG_TP.toSet,
        connected_organization_names = committee.CONNECTED_ORG_NM.toSet,
        candidate_ids = committee.CAND_ID.toSet
      )
    }

    def reduce(left: CommitteeVertex, right: CommitteeVertex): CommitteeVertex = {
      CommitteeVertex(
        uid = left.uid,
        committee_names = left.committee_names ++ right.committee_names,
        treasures_names = left.treasures_names ++ right.treasures_names,
        streets = left.streets ++ right.streets,
        cities = left.cities ++ right.cities,
        states = left.states ++ right.states,
        zip_codes = left.zip_codes ++ right.zip_codes,
        committee_designations = left.committee_designations ++ right.committee_designations,
        committee_types = left.committee_types ++ right.committee_types,
        committee_party_affiliations = left.committee_party_affiliations ++ right.committee_party_affiliations,
        interest_group_categories = left.interest_group_categories ++ right.interest_group_categories,
        connected_organization_names = left.connected_organization_names ++ right.connected_organization_names,
        candidate_ids = left.candidate_ids ++ right.candidate_ids
      )
    }

  }

}
