package in.tap.we.poli.analytic.jobs.graph.edges

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.{AggregateExpenditureEdge, ExpenditureEdge}
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor
import in.tap.we.poli.models.OperatingExpenditures
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

/**
 * We Aggregate the Edges from One Committee to One Vendor,
 * for a bread-and-butter query from DynamoDB, where the property data lives.
 */
class CommitteeToVendorEdgeJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[UniqueVendor],
  val writeTypeTagA: universe.TypeTag[AggregateExpenditureEdge]
) extends OneInOneOutJob[UniqueVendor, AggregateExpenditureEdge](inArgs, outArgs) {

  override def transform(input: Dataset[UniqueVendor]): Dataset[AggregateExpenditureEdge] = {
    import spark.implicits._
    input
      .flatMap(ExpenditureEdge.fromUniqueVendor)
      .rdd
      .reduceByKey(_ ++ _)
      .map {
        case ((srcId: VertexId, dstId: VertexId), edges: Seq[ExpenditureEdge]) =>
          AggregateExpenditureEdge(
            src_id = srcId,
            dst_id = dstId,
            num_edges = edges.size,
            edges = edges
          )
      }
      .toDS
  }

}

object CommitteeToVendorEdgeJob {

  final case class AggregateExpenditureEdge(
    src_id: VertexId,
    dst_id: VertexId,
    num_edges: BigInt,
    edges: Seq[ExpenditureEdge]
  )

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

    def fromUniqueVendor(uniqueVendor: UniqueVendor): Seq[((VertexId, VertexId), Seq[ExpenditureEdge])] = {
      uniqueVendor
        .edges
        .map { expenditureEdge: ExpenditureEdge =>
          (expenditureEdge.src_id, uniqueVendor.uid) -> Seq(expenditureEdge)
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
