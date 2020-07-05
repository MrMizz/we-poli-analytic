package in.tap.we.poli.analytic.jobs.graph.edges

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.ExpenditureEdge
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor
import in.tap.we.poli.models.OperatingExpenditures
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class CommitteeToVendorEdgeJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[UniqueVendor],
  val writeTypeTagA: universe.TypeTag[Edge[ExpenditureEdge]]
) extends OneInOneOutJob[UniqueVendor, Edge[ExpenditureEdge]](inArgs, outArgs) {

  override def transform(input: Dataset[UniqueVendor]): Dataset[Edge[ExpenditureEdge]] = {
    input.flatMap(ExpenditureEdge.fromUniqueVendor)
  }

}

object CommitteeToVendorEdgeJob {

  final case class ExpenditureEdge(
    src_id: VertexId,
    report_year: Option[Long],
    report_type: Option[String],
    form_type: Option[String],
    schedule_type: Option[String],
    transaction_date: Option[String],
    transaction_amount: Option[Double],
    primary_general_indicator: Option[String],
    disbursement_category: Option[String],
    entity_type: Option[String],
    transaction_id: Option[String],
    back_reference_transaction_number: Option[String]
  )

  object ExpenditureEdge {

    import in.tap.we.poli.analytic.jobs.graph.vertices.CommitteesVertexJob.CommitteeVertex.fromStringToLongUID

    def fromUniqueVendor(uniqueVendor: UniqueVendor): Seq[Edge[ExpenditureEdge]] = {
      uniqueVendor
        .edges
        .map { expenditureEdge: ExpenditureEdge =>
          Edge(
            srcId = expenditureEdge.src_id,
            dstId = uniqueVendor.uid,
            attr = expenditureEdge
          )
        }
        .toSeq
    }

    def fromOperatingExpenditures(operatingExpenditures: OperatingExpenditures): ExpenditureEdge = {
      ExpenditureEdge(
        src_id = fromStringToLongUID(operatingExpenditures.CMTE_ID),
        report_year = operatingExpenditures.RPT_YR,
        report_type = operatingExpenditures.RPT_TP,
        form_type = operatingExpenditures.FORM_TP_CD,
        schedule_type = operatingExpenditures.SCHED_TP_CD,
        transaction_date = operatingExpenditures.TRANSACTION_DT,
        transaction_amount = operatingExpenditures.TRANSACTION_AMT,
        primary_general_indicator = operatingExpenditures.TRANSACTION_PGI,
        disbursement_category = operatingExpenditures.CATEGORY,
        entity_type = operatingExpenditures.ENTITY_TP,
        transaction_id = operatingExpenditures.TRAN_ID,
        back_reference_transaction_number = operatingExpenditures.BACK_REF_TRAN_ID
      )
    }

  }

}
