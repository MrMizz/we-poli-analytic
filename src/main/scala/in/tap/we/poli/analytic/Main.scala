package in.tap.we.poli.analytic

import in.tap.base.spark.jobs.composite.CompositeJob
import in.tap.base.spark.main.InArgs.{OneInArgs, TwoInArgs}
import in.tap.base.spark.main.OutArgs.{OneOutArgs, TwoOutArgs}
import in.tap.base.spark.main.{InArgs, OutArgs}
import in.tap.we.poli.analytic.jobs.connectors.VendorsConnectorJob
import in.tap.we.poli.analytic.jobs.dynamo.VertexDataJob
import in.tap.we.poli.analytic.jobs.graph.NeptuneJob
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob
import in.tap.we.poli.analytic.jobs.graph.vertices.{CommitteesVertexJob, VendorsVertexJob, VerticesUnionJob}
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob
import org.apache.spark.sql.SparkSession

object Main extends in.tap.base.spark.main.Main {

  override def job(step: String, inArgs: InArgs, outArgs: OutArgs)(implicit spark: SparkSession): CompositeJob = {
    step match {
      case "vendors-transformer" =>
        new VendorsTransformerJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "vendors-connector" =>
        new VendorsConnectorJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "vendors-merger" =>
        new VendorsMergerJob(inArgs.asInstanceOf[TwoInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "vendors-vertex" =>
        new VendorsVertexJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "committees-vertex" =>
        new CommitteesVertexJob(inArgs.asInstanceOf[TwoInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "vertices-union" =>
        new VerticesUnionJob(inArgs.asInstanceOf[TwoInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "committee-to-vendor-edge" =>
        new CommitteeToVendorEdgeJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "neptune" =>
        new NeptuneJob(inArgs.asInstanceOf[TwoInArgs], outArgs.asInstanceOf[TwoOutArgs])
      case "dynamo-vertex" =>
        new VertexDataJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case _ => throw new MatchError("Invalid Step!")
    }
  }

}
