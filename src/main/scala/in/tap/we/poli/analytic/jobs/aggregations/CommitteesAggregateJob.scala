package in.tap.we.poli.analytic.jobs.aggregations

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.aggregations.CommitteesAggregateJob._
import in.tap.we.poli.models.Committee
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class CommitteesAggregateJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit spark: SparkSession,
  val writeTypeTagA: universe.TypeTag[CommitteesAggregate],
  val readTypeTagA: universe.TypeTag[Committee]
) extends OneInOneOutJob[Committee, CommitteesAggregate](inArgs, outArgs) {

  override def transform(input: Dataset[Committee]): Dataset[CommitteesAggregate] = {
    import spark.implicits._
    input.map(CommitteesAggregate.toAggregateBlock).rdd.reduceByKey(CommitteesAggregate.reduce).toDS.map(_._2)
  }

}

object CommitteesAggregateJob {

  final case class CommitteesAggregate(
    CMTE_ID: String,
    CMTE_NM: Seq[String] = Nil,
    TRES_NM: Seq[String] = Nil,
    CMTE_ST1: Seq[String] = Nil,
    CMTE_ST2: Seq[String] = Nil,
    CMTE_CITY: Seq[String] = Nil,
    CMTE_ST: Seq[String] = Nil,
    CMTE_ZIP: Seq[String] = Nil,
    CMTE_DSGN: Seq[String] = Nil,
    CMTE_TP: Seq[String] = Nil,
    CMTE_PTY_AFFILIATION: Seq[String] = Nil,
    CMTE_FILING_FREQ: Seq[String] = Nil,
    ORG_TP: Seq[String] = Nil,
    CONNECTED_ORG_NM: Seq[String] = Nil,
    CAND_ID: Seq[String] = Nil
  )

  object CommitteesAggregate {

    def toAggregateBlock(committee: Committee): (String, CommitteesAggregate) = {
      committee.CMTE_ID -> CommitteesAggregate(
        CMTE_ID = committee.CMTE_ID,
        CMTE_NM = committee.CMTE_NM.toSeq,
        TRES_NM = committee.TRES_NM.toSeq,
        CMTE_ST1 = committee.CMTE_ST1.toSeq,
        CMTE_ST2 = committee.CMTE_ST2.toSeq,
        CMTE_CITY = committee.CMTE_CITY.toSeq,
        CMTE_ST = committee.CMTE_ST.toSeq,
        CMTE_ZIP = committee.CMTE_ZIP.toSeq,
        CMTE_DSGN = committee.CMTE_DSGN.toSeq,
        CMTE_TP = committee.CMTE_TP.toSeq,
        CMTE_PTY_AFFILIATION = committee.CMTE_PTY_AFFILIATION.toSeq,
        CMTE_FILING_FREQ = committee.CMTE_FILING_FREQ.toSeq,
        ORG_TP = committee.ORG_TP.toSeq,
        CONNECTED_ORG_NM = committee.CONNECTED_ORG_NM.toSeq,
        CAND_ID = committee.CAND_ID.toSeq
      )
    }

    def reduce(left: CommitteesAggregate, right: CommitteesAggregate): CommitteesAggregate = {
      CommitteesAggregate(
        CMTE_ID = left.CMTE_ID,
        CMTE_NM = (left.CMTE_NM ++ right.CMTE_NM).distinct,
        TRES_NM = (left.TRES_NM ++ right.TRES_NM).distinct,
        CMTE_ST1 = (left.CMTE_ST1 ++ right.CMTE_ST1).distinct,
        CMTE_ST2 = (left.CMTE_ST2 ++ right.CMTE_ST2).distinct,
        CMTE_CITY = (left.CMTE_CITY ++ right.CMTE_CITY).distinct,
        CMTE_ST = (left.CMTE_ST ++ right.CMTE_ST).distinct,
        CMTE_ZIP = (left.CMTE_ZIP ++ right.CMTE_ZIP).distinct,
        CMTE_DSGN = (left.CMTE_DSGN ++ right.CMTE_DSGN).distinct,
        CMTE_TP = (left.CMTE_TP ++ right.CMTE_TP).distinct,
        CMTE_PTY_AFFILIATION = (left.CMTE_PTY_AFFILIATION ++ right.CMTE_PTY_AFFILIATION).distinct,
        CMTE_FILING_FREQ = (left.CMTE_FILING_FREQ ++ right.CMTE_FILING_FREQ).distinct,
        ORG_TP = (left.ORG_TP ++ right.ORG_TP).distinct,
        CONNECTED_ORG_NM = (left.CONNECTED_ORG_NM ++ right.CONNECTED_ORG_NM).distinct,
        CAND_ID = (left.CAND_ID ++ right.CAND_ID).distinct
      )
    }

  }

}
