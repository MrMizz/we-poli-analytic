package in.tap.we.poli.analytic.jobs.graph.edges

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.ExpenditureEdge
import in.tap.we.poli.models.OperatingExpenditures
import org.apache.spark.graphx.Edge
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class CommitteeToVendorEdgeJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[OperatingExpenditures],
  val writeTypeTagA: universe.TypeTag[Edge[ExpenditureEdge]]
) extends OneInOneOutJob[OperatingExpenditures, Edge[ExpenditureEdge]](inArgs, outArgs) {

  override def transform(input: Dataset[OperatingExpenditures]): Dataset[Edge[ExpenditureEdge]] = ???

}

object CommitteeToVendorEdgeJob {

  /**
   * TODO: extract this edge in vendors transformer.
   *  aggregate in unique vendors job.
   *  formalize here.
   * @param report_year
   * @param report_type
   * @param form_type
   * @param schedule_type
   * @param transaction_date
   * @param transaction_amount
   * @param primary_general_indicator
   * @param disbursement_category
   * @param entity_type
   * @param fec_record_number
   * @param transaction_id
   * @param back_reference_transaction_number
   */
  final case class ExpenditureEdge(
    report_year: Option[String],
    report_type: Option[String],
    form_type: Option[String],
    schedule_type: Option[String],
    transaction_date: Option[String],
    transaction_amount: Option[String],
    primary_general_indicator: Option[String],
    disbursement_category: Option[String],
    entity_type: Option[String],
    fec_record_number: Option[String],
    transaction_id: Option[String],
    back_reference_transaction_number: Option[String]
  )

}
