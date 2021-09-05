package in.tap.we.poli.analytic.jobs.dynamo.traversal

import in.tap.we.poli.analytic.jobs.dynamo.traversal.n1.GraphTraversalSB1JobSpec
import org.scalatest.Suites

class GraphTraversalPipelineSpec
    extends Suites(
      new InitJobSpec,
      new GraphTraversalSB1JobSpec,
      new GraphTraversalPageCountJobSpec
    )
