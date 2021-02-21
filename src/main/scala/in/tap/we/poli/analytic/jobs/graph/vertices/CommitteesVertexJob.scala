package in.tap.we.poli.analytic.jobs.graph.vertices

import in.tap.base.spark.jobs.composite.TwoInOneOutJob
import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.AggregateExpenditureEdge
import in.tap.we.poli.analytic.jobs.graph.vertices.CommitteesVertexJob.CommitteeVertex
import in.tap.we.poli.analytic.jobs.mergers
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.Address
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
        .flatMap(CommitteeVertex.fromCommittee)
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
      name = "DUMMY VERTEX",
      committee_names = Set.empty[String],
      treasures_names = Set.empty[String],
      address = Address.empty,
      addresses = Set.empty[Address],
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
    name: String,
    committee_names: Set[String],
    treasures_names: Set[String],
    address: Address,
    addresses: Set[Address],
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

    def fromCommittee(committee: Committee): Option[(VertexId, CommitteeVertex)] = {
      val vertexId: VertexId = {
        fromStringToLongUID(committee.CMTE_ID)
      }
      for {
        name <- committee.CMTE_NM
      } yield {
        val address: Address = {
          Address(
            street = committee.CMTE_ST1.map(_.toLowerCase),
            alternate_street = committee.CMTE_ST2.map(_.toLowerCase),
            city = committee.CMTE_CITY.map(_.toLowerCase),
            zip_code = committee.CMTE_ZIP.map(_.take(5)),
            state = committee.CMTE_ST.map(_.toLowerCase)
          )
        }
        vertexId -> CommitteeVertex(
          uid = vertexId,
          name = name.toLowerCase,
          committee_names = committee.CMTE_NM.map(_.toLowerCase).toSet,
          treasures_names = committee.TRES_NM.map(_.toLowerCase).toSet,
          address = address,
          addresses = Set(address),
          committee_designations = committee.CMTE_DSGN.map(_.toLowerCase).toSet,
          committee_types = committee.CMTE_TP.map(_.toLowerCase).toSet,
          committee_party_affiliations = committee.CMTE_PTY_AFFILIATION.map(_.toLowerCase).toSet,
          interest_group_categories = committee.ORG_TP.map(_.toLowerCase).toSet,
          connected_organization_names = committee.CONNECTED_ORG_NM.map(_.toLowerCase).toSet,
          candidate_ids = committee.CAND_ID.map(_.toLowerCase).toSet
        )
      }
    }

    def reduce(left: CommitteeVertex, right: CommitteeVertex): CommitteeVertex = {
      val names: Set[String] = {
        left.committee_names ++ right.committee_names
      }
      val addresses = {
        left.addresses ++ right.addresses
      }
      CommitteeVertex(
        uid = left.uid,
        name = mergers.getMostCommon[String](names.toSeq).getOrElse(left.name),
        committee_names = names,
        treasures_names = left.treasures_names ++ right.treasures_names,
        address = mergers.getMostCommon[Address](addresses.toSeq).getOrElse(left.address),
        addresses = addresses,
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
