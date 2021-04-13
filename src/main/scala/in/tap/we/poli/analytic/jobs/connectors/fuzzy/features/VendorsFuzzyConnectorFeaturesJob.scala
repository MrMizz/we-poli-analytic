package in.tap.we.poli.analytic.jobs.connectors.fuzzy.features

import in.tap.base.spark.jobs.composite.TwoInOneOutJob
import in.tap.base.spark.main.InArgs.TwoInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.VendorsFuzzyConnectorFeaturesJob.{
  buildSamplingRatio, SampleBuilder
}
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.transfomer.IdResVendorTransformerJob.IdResVendor
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe

// TODO: fix typo in parent class
class VendorsFuzzyConnectorFeaturesJob(val inArgs: TwoInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[IdResVendor],
  val readTypeTagB: universe.TypeTag[(VertexId, VertexId)],
  val writeTypeTagA: universe.TypeTag[(Long, Comparison)]
) extends TwoInOneOutJob[IdResVendor, (VertexId, VertexId), (Long, Comparison)](inArgs, outArgs) {

  override def transform(input: (Dataset[IdResVendor], Dataset[(VertexId, VertexId)])): Dataset[(Long, Comparison)] = {
    import spark.implicits._
    val (vendors, connector) = {
      input
    }
    val positives: RDD[Comparison] = {
      SampleBuilder.positives(vendors, connector)
    }
    val negatives: RDD[Comparison] = {
      SampleBuilder.negatives(vendors, connector)
    }
    val numPositives: Double = {
      positives.count.toDouble
    }
    val numNegatives: Double = {
      negatives.count.toDouble
    }
    positives
      .map { comparison: Comparison =>
        1L -> comparison
      }
      .sample(
        withReplacement = true,
        fraction = buildSamplingRatio(numPositives, numNegatives)
      )
      .union(
        negatives.map { comparison: Comparison =>
          0L -> comparison
        }
      )
      .toDS
  }

}

object VendorsFuzzyConnectorFeaturesJob {

  val POS_TO_NEG_RATIO: Double = {
    1.0
  }

  val MAX_COMPARISON_SIZE: Int = {
    100
  }

  def buildSamplingRatio(numPositives: Double, numNegatives: Double): Double = {
    (numNegatives * POS_TO_NEG_RATIO) / numPositives
  }

  object SampleBuilder {

    def positives(vendors: Dataset[IdResVendor], connector: Dataset[(VertexId, VertexId)])(
      implicit spark: SparkSession
    ): RDD[Comparison] = {
      CandidateReducer[VertexId, Comparator](
        join(vendors, connector)
      ).flatMap { maybe =>
        Comparison(
          maybe.toList.flatten
        )
      }
    }

    def negatives(vendors: Dataset[IdResVendor], connector: Dataset[(VertexId, VertexId)])(
      implicit spark: SparkSession
    ): RDD[Comparison] = {
      import spark.implicits._
      CandidateGenerator(
        join(vendors, connector).map(_._2).toDS
      )
    }

    private def join(vendors: Dataset[IdResVendor], connector: Dataset[(VertexId, VertexId)])(
      implicit spark: SparkSession
    ): RDD[(VertexId, Comparator)] = {
      import spark.implicits._
      vendors
        .map { vendor: IdResVendor =>
          vendor.uid -> vendor
        }
        .rdd
        .join {
          connector.rdd
        }
        .map {
          case (_, (vendor: IdResVendor, ccid: VertexId)) =>
            (ccid, Comparator(vendor, ccid))
        }
    }

  }

  private object CandidateReducer {

    def apply[A: ClassTag, B](rdd: RDD[(A, B)])(
      implicit spark: SparkSession
    ): RDD[Option[List[B]]] = {
      val pair: PairRDDFunctions[A, Option[List[B]]] = {
        new PairRDDFunctions[A, Option[List[B]]](
          rdd
            .map {
              case (key, value) =>
                key -> Option(List(value))
            }
        )
      }
      pair
        .reduceByKey(reduce)
        .map {
          case (_, candidates) =>
            candidates
        }
    }

    private def reduce[A](left: Option[List[A]], right: Option[List[A]]): Option[List[A]] = {
      (left, right) match {
        case (Some(l), Some(r)) =>
          if ((l.size + r.size) <= MAX_COMPARISON_SIZE) {
            Some(l ++ r)
          } else {
            None
          }
        case _ => None
      }
    }

  }

  private object CandidateGenerator {

    def apply(vendors: Dataset[Comparator])(implicit spark: SparkSession): RDD[Comparison] = {
      import spark.implicits._
      val comparators: RDD[(String, Comparator)] = {
        vendors
          .flatMap { comparator: Comparator =>
            comparator.cgTokens.map { token: String =>
              (token, comparator.ccid) -> comparator
            }
          }
          .rdd
          .reduceByKey {
            case (left, _) =>
              left
          }
          .map {
            case ((token, _), comparator: Comparator) =>
              (token, comparator)
          }
      }
      CandidateReducer(comparators).flatMap { maybe =>
        Comparison(maybe.toList.flatten)
      }
    }

  }

}
