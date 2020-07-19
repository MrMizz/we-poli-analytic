package in.tap.we.poli.analytic.jobs.graph.vertices

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.graph.vertices.CommitteesVertexJob.CommitteeVertex
import in.tap.we.poli.models.Committee
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

/**
 * TODO:
 *  union with Unique Vendors.
 *  there are a few cases where vendors appear in Operating Expenditures
 *  that are not listed in the Committee Master File.
 * @param inArgs
 * @param outArgs
 * @param spark
 * @param readTypeTagA
 * @param writeTypeTagA
 */
class CommitteesVertexJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[Committee],
  val writeTypeTagA: universe.TypeTag[CommitteeVertex]
) extends OneInOneOutJob[Committee, CommitteeVertex](inArgs, outArgs) {

  override def transform(input: Dataset[Committee]): Dataset[CommitteeVertex] = {
    import spark.implicits._
    input.map(CommitteeVertex.fromCommittee).rdd.reduceByKey(CommitteeVertex.reduce).map(_._2).toDS
  }

}

object CommitteesVertexJob {

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
