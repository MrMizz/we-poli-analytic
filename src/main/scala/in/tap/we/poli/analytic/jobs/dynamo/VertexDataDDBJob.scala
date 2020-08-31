package in.tap.we.poli.analytic.jobs.dynamo

import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.base.spark.populator.DynamoPopulator
import in.tap.we.poli.analytic.jobs.graph.vertices.VerticesUnionJob.AgnosticVertex
import org.apache.spark.sql.SparkSession

class VertexDataDDBJob(override val inArgs: OneInArgs, override val outArgs: OneOutArgs)(
  implicit override val spark: SparkSession
) extends DynamoPopulator[AgnosticVertex](inArgs, outArgs)
