package in.tap.we.poli.analytic.jobs.dynamo.traversal

import org.scalatest.Suites

class GraphTraversalPipelineSpec
    extends Suites(
      new InitJobSpec,
      new n1.GraphTraversalSB1JobSpec,
      new n2.GraphTraversalSB1JobSpec,
      new n3.GraphTraversalSB1JobSpec,
      new GraphTraversalPageCountJobSpec
    )
