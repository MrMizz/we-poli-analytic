package in.tap.we.poli.analytic.jobs.connectors

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.connectors.VendorsComparisonJob.{VendorsComparator, VendorsComparison}
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class VendorsComparisonJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[UniqueVendor],
  val writeTypeTagA: universe.TypeTag[VendorsComparison]
) extends OneInOneOutJob[UniqueVendor, VendorsComparison](inArgs, outArgs) {

  override def transform(input: Dataset[UniqueVendor]): Dataset[VendorsComparison] = {
    import spark.implicits._
    input
      .flatMap(VendorsComparator.apply)
      .rdd
      .reduceByKey(VendorsComparator.reduce)
      .flatMap {
        case (_, comparators: Seq[(Long, VendorsComparator)]) =>
          VendorsComparison(comparators.map(_._2))
      }
      .toDS
  }

}

object VendorsComparisonJob {

  val MAX_COMPARISON_SIZE: Int = {
    100
  }

  final case class VendorsComparison(
    left: VendorsComparator,
    right: VendorsComparator,
    num_name_tokens_in_common: Long,
    num_total_merged: Long
  )

  object VendorsComparison {

    def apply(comparators: Seq[VendorsComparator]): Seq[VendorsComparison] = {
      comparators match {
        case Nil      => Nil
        case _ :: Nil => Nil
        case seq =>
          seq.combinations(n = 2).toSeq.flatMap { c: Seq[VendorsComparator] =>
            c match {
              case head :: tail :: Nil =>
                Some(
                  VendorsComparison(
                    left = head,
                    right = tail,
                    num_name_tokens_in_common = numNameTokensInCommon(head, tail),
                    num_total_merged = head.num_merged + tail.num_merged
                  )
                )
              case _ => None
            }
          }
      }
    }

    private def numNameTokensInCommon(left: VendorsComparator, right: VendorsComparator): Long = {
      import ConnectorUtils.cleanedNameTokens
      cleanedNameTokens(left.name).toSet.intersect(cleanedNameTokens(right.name).toSet).size
    }

  }

  final case class VendorsComparator(
    uid: Long,
    name: String,
    city: Option[String],
    state: Option[String],
    num_merged: Long
  )

  object VendorsComparator {

    def apply(uniqueVendor: UniqueVendor): Seq[(String, Seq[(Long, VendorsComparator)])] = {
      val vendorsComparator: VendorsComparator = {
        new VendorsComparator(
          uid = uniqueVendor.uid,
          name = uniqueVendor.name,
          city = uniqueVendor.city,
          state = uniqueVendor.state,
          num_merged = uniqueVendor.num_merged.toLong
        )
      }
      ConnectorUtils.cleanedNameTokens(uniqueVendor.name).map { token: String =>
        token -> Seq(1L -> vendorsComparator)
      }
    }

    def reduce(
      left: Seq[(Long, VendorsComparator)],
      right: Seq[(Long, VendorsComparator)]
    ): Seq[(Long, VendorsComparator)] = {
      val cat: Seq[(Long, VendorsComparator)] = {
        (left ++ right).sortBy(_._2.num_merged)
      }
      if (cat.size > MAX_COMPARISON_SIZE) {
        cat.takeRight(MAX_COMPARISON_SIZE)
      } else {
        cat
      }
    }

  }

}
