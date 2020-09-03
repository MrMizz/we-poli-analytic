package in.tap.we.poli.analytic.jobs.dynamo

import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.base.spark.populator.DynamoPopulator
import in.tap.we.poli.analytic.jobs.dynamo.EdgeDataJob.EdgeData
import org.apache.spark.sql.SparkSession

class EdgeDataDDBJob(override val inArgs: OneInArgs, override val outArgs: OneOutArgs)(
  implicit override val spark: SparkSession
) extends DynamoPopulator[EdgeData](inArgs, outArgs)
