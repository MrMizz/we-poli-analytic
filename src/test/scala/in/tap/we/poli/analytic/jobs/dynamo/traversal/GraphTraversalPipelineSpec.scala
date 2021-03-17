package in.tap.we.poli.analytic.jobs.dynamo.traversal

import org.scalatest.Suites

class GraphTraversalPipelineSpec extends Suites(new GraphTraversalSB1JobSpec, new GraphTraversalPageCountJobSpec)
