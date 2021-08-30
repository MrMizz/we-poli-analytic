package in.tap.we.poli.analytic

import in.tap.base.spark.jobs.composite.CompositeJob
import in.tap.base.spark.main.InArgs.{OneInArgs, TwoInArgs}
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.base.spark.main.{InArgs, OutArgs}
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.VendorsFuzzyConnectorJob
import in.tap.we.poli.analytic.jobs.connectors.auto.VendorsAutoConnectorJob
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.{Comparison, VendorsFuzzyConnectorFeaturesJob}
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.predictor.VendorsFuzzyPredictorJob
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.training.VendorsFuzzyConnectorTrainingJob
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.transfomer.IdResVendorTransformerJob
import in.tap.we.poli.analytic.jobs.connectors.unify.ConnectorsUnifyJob
import in.tap.we.poli.analytic.jobs.dynamo.autocomplete.{VertexNameAutoCompleteDDBJob, VertexNameAutoCompleteJob}
import in.tap.we.poli.analytic.jobs.dynamo.edge.{EdgeDataDDBJob, EdgeDataJob}
import in.tap.we.poli.analytic.jobs.dynamo.traversal.{
  GraphTraversalPageCountDDBJob, GraphTraversalPageCountJob, GraphTraversalPageDDBJob
}
import in.tap.we.poli.analytic.jobs.dynamo.traversal.{n1, n2, n3, n4, n5}
import in.tap.we.poli.analytic.jobs.dynamo.vertex.VertexDataDDBJob
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
      case "vendors-auto-connector" =>
        new VendorsAutoConnectorJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "id-res-vendors" =>
        new IdResVendorTransformerJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "vendors-fuzzy-connector" =>
        new VendorsFuzzyConnectorJob(inArgs.asInstanceOf[TwoInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "connectors-union" =>
        new ConnectorsUnifyJob(inArgs.asInstanceOf[TwoInArgs], outArgs.asInstanceOf[OneOutArgs])
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
      // n1 traversals
      case "dynamo-graph-traversal-n1-sb1" =>
        new n1.GraphTraversalSB1Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-n1-sb2" =>
        new n1.GraphTraversalSB2Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-n1-sb3" =>
        new n1.GraphTraversalSB3Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-n1-sb4" =>
        new n1.GraphTraversalSB4Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-n1-sb5" =>
        new n1.GraphTraversalSB5Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      // n2 traversals
      case "dynamo-graph-traversal-n2-sb1" =>
        new n2.GraphTraversalSB1Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-n2-sb2" =>
        new n2.GraphTraversalSB2Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-n2-sb3" =>
        new n2.GraphTraversalSB3Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-n2-sb4" =>
        new n2.GraphTraversalSB4Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-n2-sb5" =>
        new n2.GraphTraversalSB5Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      // n3 traversals
      case "dynamo-graph-traversal-n3-sb1" =>
        new n3.GraphTraversalSB1Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-n3-sb2" =>
        new n3.GraphTraversalSB2Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-n3-sb3" =>
        new n3.GraphTraversalSB3Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-n3-sb4" =>
        new n3.GraphTraversalSB4Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-n3-sb5" =>
        new n3.GraphTraversalSB5Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      // n4 traversals
      case "dynamo-graph-traversal-n4-sb1" =>
        new n4.GraphTraversalSB1Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-n4-sb2" =>
        new n4.GraphTraversalSB2Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-n4-sb3" =>
        new n4.GraphTraversalSB3Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-n4-sb4" =>
        new n4.GraphTraversalSB4Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-n4-sb5" =>
        new n4.GraphTraversalSB5Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      // n5 traversals
      case "dynamo-graph-traversal-n5-sb1" =>
        new n5.GraphTraversalSB1Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-n5-sb2" =>
        new n5.GraphTraversalSB2Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-n5-sb3" =>
        new n5.GraphTraversalSB3Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-n5-sb4" =>
        new n5.GraphTraversalSB4Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-n5-sb5" =>
        new n5.GraphTraversalSB5Job(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      // traversal page writers
      case "dynamo-graph-traversal-page-writer" =>
        new GraphTraversalPageDDBJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-page-count" =>
        new GraphTraversalPageCountJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "dynamo-graph-traversal-page-count-writer" =>
        new GraphTraversalPageCountDDBJob(inArgs.asInstanceOf[OneInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "id-res-features" =>
        new VendorsFuzzyConnectorFeaturesJob(inArgs.asInstanceOf[TwoInArgs], outArgs.asInstanceOf[OneOutArgs])
      case "id-res-name-training" =>
        new VendorsFuzzyConnectorTrainingJob(
          inArgs.asInstanceOf[OneInArgs],
          outArgs.asInstanceOf[OneOutArgs],
          (c: Comparison) => c.nameFeatures.toArray
        )
      case "id-res-address-training" =>
        new VendorsFuzzyConnectorTrainingJob(
          inArgs.asInstanceOf[OneInArgs],
          outArgs.asInstanceOf[OneOutArgs],
          (c: Comparison) => c.addressFeatures.toArray
        )
      case "id-res-transaction-training" =>
        new VendorsFuzzyConnectorTrainingJob(
          inArgs.asInstanceOf[OneInArgs],
          outArgs.asInstanceOf[OneOutArgs],
          (c: Comparison) => c.transactionFeatures.toArray
        )
      case "id-res-composite-training" =>
        new VendorsFuzzyConnectorTrainingJob(
          inArgs.asInstanceOf[OneInArgs],
          outArgs.asInstanceOf[OneOutArgs],
          (c: Comparison) => c.compositeFeatures.toArray
        )
      case "id-res-predictor" =>
        new VendorsFuzzyPredictorJob(inArgs.asInstanceOf[TwoInArgs], outArgs.asInstanceOf[OneOutArgs])
      case _ => throw new MatchError("Invalid Step!")
    }
  }

}
