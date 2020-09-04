package in.tap.we.poli.analytic.jobs.dynamo

import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.base.spark.populator.DynamoPopulator
import in.tap.we.poli.analytic.jobs.dynamo.GraphTraversalJob.GraphTraversal
import org.apache.spark.sql.SparkSession

class GraphTraversalDDBJob(override val inArgs: OneInArgs, override val outArgs: OneOutArgs)(
  implicit override val spark: SparkSession
) extends DynamoPopulator[GraphTraversal](inArgs, outArgs)
