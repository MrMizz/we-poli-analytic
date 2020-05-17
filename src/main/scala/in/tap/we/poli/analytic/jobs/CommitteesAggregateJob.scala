package in.tap.we.poli.analytic.jobs

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.CommitteesAggregate
import in.tap.we.poli.models.Committee
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class CommitteesAggregateJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit spark: SparkSession,
  val writeTypeTagA: universe.TypeTag[CommitteesAggregate],
  val readTypeTagA: universe.TypeTag[Committee]
) extends OneInOneOutJob[Committee, CommitteesAggregate](inArgs, outArgs) {

  override def transform(input: Dataset[Committee]): Dataset[CommitteesAggregate] = {
    import spark.implicits._
    input.map(CommitteesAggregate.toAggregateBlock).rdd.reduceByKey(CommitteesAggregate.reduce).toDS.map(_._2)
  }

}
