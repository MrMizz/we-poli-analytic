package in.tap.we.poli.analytic.jobs.dynamo.traversal.n3

import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.dynamo.traversal.nx.NxInitJob
import in.tap.we.poli.analytic.jobs.dynamo.traversal.nx.NxKey.{N2Key, N3Key}
import org.apache.spark.sql.SparkSession

class N3InitJob(
  override val inArgs: TwoInArgs,
  override val outArgs: OneOutArgs
)(
  implicit override val spark: SparkSession
) extends NxInitJob[N2Key, N3Key](
      inArgs,
      outArgs,
      N3Key.apply
    )
