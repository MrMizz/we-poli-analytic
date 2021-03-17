package in.tap.we.poli.analytic

import in.tap.base.spark.jobs.composite.CompositeJob
import in.tap.base.spark.main.InArgs.{OneInArgs, ThreeInArgs, TwoInArgs}
import in.tap.base.spark.main.OutArgs.{OneOutArgs, TwoOutArgs}
import in.tap.base.spark.main.{InArgs, OutArgs}
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.{
  VendorsFuzzyConnectorFeaturesJob, VendorsFuzzyConnectorJob, VendorsFuzzyConnectorTrainingJob, VendorsFuzzyPredictorJob
}
import in.tap.we.poli.analytic.jobs.connectors.auto.VendorsAutoConnectorJob
import in.tap.we.poli.analytic.jobs.dynamo.autocomplete.{VertexNameAutoCompleteDDBJob, VertexNameAutoCompleteJob}
import in.tap.we.poli.analytic.jobs.dynamo.edge.{EdgeDataDDBJob, EdgeDataJob}
import in.tap.we.poli.analytic.jobs.dynamo.traversal.{
  GraphTraversalPageCountDDBJob, GraphTraversalPageDDBJob, GraphTraversalSB1Job, GraphTraversalSB2Job,
  GraphTraversalSB3Job, GraphTraversalSB4Job, GraphTraversalSB5Job
}
import in.tap.we.poli.analytic.jobs.dynamo.vertex.VertexDataDDBJob
import in.tap.we.poli.analytic.jobs.graph.NeptuneJob
import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob
import in.tap.we.poli.analytic.jobs.graph.vertices.{CommitteesVertexJob, VendorsVertexJob, VerticesUnionJob}
import in.tap.we.poli.analytic.jobs.mergers.{UniqueVendorsMergerJob, VendorsMergerJob}
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob
import org.apache.spark.sql.SparkSession

object Main extends in.tap.base.spark.main.Main {

  override def job(step: String, inArgs: InArgs, outArgs: OutArgs)(implicit spark: SparkSession): CompositeJob = {
    step match {
      case "vendors-transformer" =>
        new VendorsTransformerJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "vendors-connector" =>
        new VendorsAutoConnectorJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "vendors-merger" =>
        new VendorsMergerJob(inArgs.asInstanceOf[TwoInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "unique-vendors-connector" =>
        new VendorsFuzzyConnectorJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "unique-vendors-merger" =>
        new UniqueVendorsMergerJob(inArgs.asInstanceOf[TwoInArgs], outArgs.asInstanceOf[OneOutArgs])
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
      case "dynamo-graph-traversal-sb1" =>
        new GraphTraversalSB1Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-sb2" =>
        new GraphTraversalSB2Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-sb3" =>
        new GraphTraversalSB3Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-sb4" =>
        new GraphTraversalSB4Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-sb5" =>
        new GraphTraversalSB5Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-page-writer" =>
        new GraphTraversalPageDDBJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-page-count-writer" =>
        new GraphTraversalPageCountDDBJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "fuzzy-connector-features" =>
        new VendorsFuzzyConnectorFeaturesJob(inArgs.asInstanceOf[ThreeInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "fuzzy-connector-training" =>
        new VendorsFuzzyConnectorTrainingJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "fuzzy-predictor" =>
        new VendorsFuzzyPredictorJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case _ => throw new MatchError("Invalid Step!")
    }
  }

}
