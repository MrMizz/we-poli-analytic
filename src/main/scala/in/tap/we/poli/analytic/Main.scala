package in.tap.we.poli.analytic

import in.tap.base.spark.jobs.composite.CompositeJob
import in.tap.base.spark.main.InArgs.{OneInArgs, ThreeInArgs, TwoInArgs}
import in.tap.base.spark.main.OutArgs.{OneOutArgs, TwoOutArgs}
import in.tap.base.spark.main.{InArgs, OutArgs}
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.VendorsFuzzyConnectorFeaturesJob
import in.tap.we.poli.analytic.jobs.connectors.{VendorsComparisonJob, VendorsConnectorJob}
import in.tap.we.poli.analytic.jobs.dynamo.{
  EdgeDataDDBJob, EdgeDataJob, GraphTraversalJob, GraphTraversalPageCountDDBJob, GraphTraversalPageDDBJob,
  VertexDataDDBJob, VertexNameAutoCompleteDDBJob, VertexNameAutoCompleteJob
}
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
      case "vendors-comparison" =>
        new VendorsComparisonJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
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
      case "dynamo-vertex-name" =>
        new VertexNameAutoCompleteJob(
          inArgs.asInstanceOf[TwoInArgs],
          outArgs.asInstanceOf[OneOutArgs],
          MAX_RESPONSE_SIZE = 25
        )
      case "dynamo-vertex-name-writer" =>
        new VertexNameAutoCompleteDDBJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-vertex-data-writer" =>
        new VertexDataDDBJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-edge-data" =>
        new EdgeDataJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-edge-data-writer" =>
        new EdgeDataDDBJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal" =>
        new GraphTraversalJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[TwoOutArgs])
      case "dynamo-graph-traversal-page-writer" =>
        new GraphTraversalPageDDBJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-page-count-writer" =>
        new GraphTraversalPageCountDDBJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "fuzzy-connector-features" =>
        new VendorsFuzzyConnectorFeaturesJob(inArgs.asInstanceOf[ThreeInArgs], outArgs.asInstanceOf[OneOutArgs])
      case _ => throw new MatchError("Invalid Step!")
    }
  }

}
