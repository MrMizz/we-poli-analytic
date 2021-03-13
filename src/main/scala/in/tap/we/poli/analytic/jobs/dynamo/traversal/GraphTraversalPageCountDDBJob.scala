package in.tap.we.poli.analytic.jobs.dynamo.traversal

import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.base.spark.populator.DynamoPopulator
import in.tap.we.poli.analytic.jobs.dynamo.traversal.GraphTraversalJob.GraphTraversalPageCount
import org.apache.spark.sql.SparkSession

class GraphTraversalPageCountDDBJob(override val inArgs: OneInArgs, override val outArgs: OneOutArgs)(
  implicit override val spark: SparkSession
) extends DynamoPopulator[GraphTraversalPageCount](inArgs, outArgs)
