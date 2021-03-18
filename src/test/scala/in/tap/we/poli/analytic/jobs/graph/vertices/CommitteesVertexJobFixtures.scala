package in.tap.we.poli.analytic.jobs.graph.vertices

import in.tap.we.poli.analytic.jobs.graph.vertices.CommitteesVertexJob.CommitteeVertex
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.Address
import in.tap.we.poli.models.Committee

trait CommitteesVertexJobFixtures {

  val committee1: Committee = {
    Committee(
      CMTE_ID = "C1",
      CMTE_NM = Some("committee1"),
      TRES_NM = Some("treasure1"),
      CMTE_ST1 = Some("street1"),
      CMTE_ST2 = Some("street2"),
      CMTE_CITY = Some("city1"),
      CMTE_ST = Some("state1"),
      CMTE_ZIP = Some("zip1"),
      CMTE_DSGN = Some("designation1"),
      CMTE_TP = Some("type1"),
      CMTE_PTY_AFFILIATION = Some("affiliation1"),
      CMTE_FILING_FREQ = Some("frequency1"),
      ORG_TP = Some("organization1"),
      CONNECTED_ORG_NM = Some("connection1"),
      CAND_ID = Some("candidate1")
    )
  }

  val committee2: Committee = {
    Committee(
      CMTE_ID = "C1",
      CMTE_NM = None,
      TRES_NM = None,
      CMTE_ST1 = None,
      CMTE_ST2 = None,
      CMTE_CITY = None,
      CMTE_ST = None,
      CMTE_ZIP = None,
      CMTE_DSGN = None,
      CMTE_TP = None,
      CMTE_PTY_AFFILIATION = None,
      CMTE_FILING_FREQ = None,
      ORG_TP = None,
      CONNECTED_ORG_NM = None,
      CAND_ID = None
    )
  }

  val committeeVertex1: CommitteeVertex = {
    val address = {
      Address(
        street = Option("street1"),
        alternate_street = Option("street2"),
        city = Option("city1"),
        state = Option("state1"),
        zip_code = Option("zip1")
      )
    }
    CommitteeVertex(
      uid = 11L,
      name = "committee1",
      committee_names = Set("committee1"),
      treasures_names = Set("treasure1"),
      address = address,
      addresses = Set(address),
      committee_designations = Set("designation1"),
      committee_types = Set("type1"),
      committee_party_affiliations = Set("affiliation1"),
      interest_group_categories = Set("organization1"),
      connected_organization_names = Set("connection1"),
      candidate_ids = Set("candidate1")
    )
  }

  val committeeVertex2: CommitteeVertex = {
    CommitteeVertex(
      uid = 11L,
      name = "committee2",
      committee_names = Set(),
      treasures_names = Set(),
      address = Address.empty,
      addresses = Set.empty[Address],
      committee_designations = Set(),
      committee_types = Set(),
      committee_party_affiliations = Set(),
      interest_group_categories = Set(),
      connected_organization_names = Set(),
      candidate_ids = Set()
    )
  }

  val committeeVertex3: CommitteeVertex = {
    val address = {
      Address(
        street = Option("street1"),
        alternate_street = Option("street3"),
        city = Option("city3"),
        state = None,
        zip_code = Option("zip1")
      )
    }
    CommitteeVertex(
      uid = 11L,
      name = "committee3",
      committee_names = Set("committee3"),
      treasures_names = Set("treasure3"),
      address = address,
      addresses = Set(address),
      committee_designations = Set("designation1"),
      committee_types = Set("type1"),
      committee_party_affiliations = Set("affiliation1"),
      interest_group_categories = Set("organization1"),
      connected_organization_names = Set("connection1"),
      candidate_ids = Set("candidate1")
    )
  }

  val committeeVertex4: CommitteeVertex = {
    val address1 = {
      Address(
        street = Option("street1"),
        alternate_street = Option("street2"),
        city = Option("city1"),
        state = Option("state1"),
        zip_code = Option("zip1")
      )
    }
    val address2 = {
      Address(
        street = Option("street1"),
        alternate_street = Option("street3"),
        city = Option("city3"),
        state = None,
        zip_code = Option("zip1")
      )
    }
    CommitteeVertex(
      uid = 11L,
      name = "committee1",
      committee_names = Set("committee1", "committee3"),
      treasures_names = Set("treasure1", "treasure3"),
      address = address2,
      addresses = Set(address1, address2),
      committee_designations = Set("designation1"),
      committee_types = Set("type1"),
      committee_party_affiliations = Set("affiliation1"),
      interest_group_categories = Set("organization1"),
      connected_organization_names = Set("connection1"),
      candidate_ids = Set("candidate1")
    )
  }

}
