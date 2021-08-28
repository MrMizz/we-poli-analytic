package in.tap.we.poli.analytic.jobs.dynamo.traversal.n1

import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.base.spark.populator.DynamoPopulator
import in.tap.we.poli.analytic.jobs.dynamo.traversal.Traversal
import org.apache.spark.sql.SparkSession

class GraphTraversalPageDDBJob(override val inArgs: OneInArgs, override val outArgs: OneOutArgs)(
  implicit override val spark: SparkSession
) extends DynamoPopulator[Traversal](inArgs, outArgs)
