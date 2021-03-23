package in.tap.we.poli.analytic.jobs.connectors.fuzzy.features

import in.tap.base.spark.jobs.composite.ThreeInOnOutJob
import in.tap.base.spark.main.InArgs.ThreeInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.connectors.cleanedNameTokens
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.VendorsFuzzyConnectorJob.CandidateGenerator
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.VendorsFuzzyConnectorFeaturesJob.{
  buildSamplingRatio, CandidateReducer, Comparator, Comparison, Features
}
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.transfomer.IdResVendorTransformerJob.IdResVendor
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

// TODO: fix typo in parent class
class VendorsFuzzyConnectorFeaturesJob(val inArgs: ThreeInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[IdResVendor],
  val readTypeTagB: universe.TypeTag[(VertexId, VertexId)],
  val readTypeTagC: universe.TypeTag[IdResVendor],
  val writeTypeTagA: universe.TypeTag[(Long, Features)]
) extends ThreeInOnOutJob[IdResVendor, (VertexId, VertexId), IdResVendor, (Long, Features)](inArgs, outArgs) {

  override def transform(
    input: (Dataset[IdResVendor], Dataset[(VertexId, VertexId)], Dataset[IdResVendor])
  ): Dataset[(Long, Features)] = {
    import spark.implicits._
    val (vendors: Dataset[IdResVendor], connector: Dataset[(VertexId, VertexId)], uniqueVendors: Dataset[IdResVendor]) = {
      input
    }
    val vendorComparisons: RDD[Comparison] = {
      val comparators: RDD[(VertexId, Option[Seq[Comparator]])] = {
        vendors
          .map { vendor: IdResVendor =>
            vendor.uid -> Option(Seq(Comparator(vendor)))
          }
          .rdd
          .join(
            connector.rdd
          )
          .map {
            case (_, (vendor: Option[Seq[Comparator]], connectedId: VertexId)) =>
              connectedId -> vendor
          }
      }
      CandidateReducer(comparators)
    }
    val uniqueVendorComparisons: RDD[Comparison] = {
      CandidateGenerator(uniqueVendors)
    }
    val numPositives: Double = {
      vendorComparisons.count.toDouble
    }
    val numNegatives: Double = {
      uniqueVendorComparisons.count.toDouble
    }
    vendorComparisons
      .map { comparison: Comparison =>
        1L -> comparison.features
      }
      .sample(
        withReplacement = false,
        fraction = buildSamplingRatio(numPositives, numNegatives)
      )
      .union(
        uniqueVendorComparisons.map { comparison: Comparison =>
          0L -> comparison.features
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

  object CandidateReducer {

    def apply[A](rdd: PairRDDFunctions[A, Option[Seq[Comparator]]])(
      implicit spark: SparkSession
    ): RDD[Comparison] = {
      rdd
        .reduceByKey(reduce[Comparator])
        .flatMap {
          case (_, candidates: Option[Seq[Comparator]]) =>
            unpack(candidates)
        }
    }

    private def reduce[A](left: Option[Seq[A]], right: Option[Seq[A]]): Option[Seq[A]] = {
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

    private def unpack(maybeSeq: Option[Seq[Comparator]]): Seq[Comparison] = {
      maybeSeq match {
        case Some(seq) => Comparison(seq)
        case None      => Nil
      }
    }

  }

  // TODO: scale numEdgesInCommon
  // TODO: 0, 1, more than 1 -> categorical
  final case class Features(
    numTokens: Double,
    numTokensInCommon: Double,
    numSrcIdsInCommon: Double,
    sameCity: Double,
    sameZip: Double,
    sameState: Double
  ) {

    def toArray: Array[Double] = {
      Array(
        numTokens,
        numTokensInCommon,
        numSrcIdsInCommon,
        sameCity,
        sameZip,
        sameState
      )
    }

  }

  object Features {

    /**
     * Intended for use with [[Features.numSrcIdsInCommon]].
     * Raw -> Categorical Feature.
     */
    def scale(raw: Double): Double = {
      raw match {
        case 0 => 0
        case 1 => 1
        case _ => 2
      }
    }

  }

  final case class Comparator(
    vendor: IdResVendor
  ) {

    val nameTokens: Set[String] = {
      vendor.names.flatMap(cleanedNameTokens)
    }

    val addressTokens: Set[String] = {
      vendor.addresses.flatMap(_.city) ++
        vendor.addresses.flatMap(_.zip_code)
    }

    val srcIdTokens: Set[String] = {
      vendor.src_ids.map(_.toString)
    }

    val cgTokens: Set[String] = {
      nameTokens ++
        addressTokens ++
        srcIdTokens
    }

  }

  final case class Comparison(
    left_side: Comparator,
    right_side: Comparator
  ) {

    lazy val features: Features = {
      Features(
        numTokens = numTokens,
        numTokensInCommon = numTokensInCommon,
        numSrcIdsInCommon = numSrcIdsInCommon,
        sameCity = toDouble(sameCity),
        sameZip = toDouble(sameZip),
        sameState = toDouble(sameState)
      )
    }

    private lazy val numTokens: Double = {
      Seq(left_side.nameTokens.size, right_side.nameTokens.size).max.toDouble
    }

    // TODO: Validate for city/state
    private lazy val numTokensInCommon: Double = {
      left_side.nameTokens.intersect(right_side.nameTokens).size.toDouble
    }

    private lazy val numSrcIdsInCommon: Double = {
      Features.scale(
        left_side.srcIdTokens.intersect(right_side.srcIdTokens).size.toDouble
      )
    }

    private lazy val sameCity: Boolean = {
      same(_.addresses.flatMap(_.city))
    }

    private lazy val sameZip: Boolean = {
      same(_.addresses.flatMap(_.zip_code))
    }

    private lazy val sameState: Boolean = {
      same(_.addresses.flatMap(_.state))
    }

    private def toDouble(bool: Boolean): Double = {
      bool.compare(false)
    }

    private def same(f: IdResVendor => Set[String]): Boolean = {
      val left: Set[String] = {
        f(left_side.vendor).map(_.toLowerCase)
      }
      val right = {
        f(right_side.vendor).map(_.toLowerCase)
      }
      left.intersect(right).size match {
        case 0 => false
        case _ => true
      }
    }

  }

  object Comparison {

    def apply(seq: Seq[Comparator]): Seq[Comparison] = {
      seq.combinations(n = 2).toSeq.flatMap { combination: Seq[Comparator] =>
        combination match {
          case left :: right :: Nil => Some(Comparison(left, right))
          case _                    => None
        }
      }
    }

  }

}
