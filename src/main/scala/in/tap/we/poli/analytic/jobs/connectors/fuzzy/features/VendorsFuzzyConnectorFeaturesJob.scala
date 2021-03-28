package in.tap.we.poli.analytic.jobs.connectors.fuzzy.features

import in.tap.base.spark.jobs.composite.ThreeInOnOutJob
import in.tap.base.spark.main.InArgs.ThreeInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.connectors.cleanedNameTokens
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.VendorsFuzzyConnectorJob.CandidateGenerator
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.VendorsFuzzyConnectorFeaturesJob.{
  buildSamplingRatio, CandidateReducer, Comparator, Comparison, Features
}
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.transfomer.IdResVendorTransformerJob
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

// TODO: fix typo in parent class
class VendorsFuzzyConnectorFeaturesJob(val inArgs: ThreeInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[IdResVendorTransformerJob.Source.Vendor],
  val readTypeTagB: universe.TypeTag[(VertexId, VertexId)],
  val readTypeTagC: universe.TypeTag[IdResVendorTransformerJob.Source.UniqueVendor],
  val writeTypeTagA: universe.TypeTag[(Long, Features)]
) extends ThreeInOnOutJob[
      IdResVendorTransformerJob.Source.Vendor,
      (VertexId, VertexId),
      IdResVendorTransformerJob.Source.UniqueVendor,
      (Long, Features)
    ](inArgs, outArgs) {

  override def transform(
    input: (
      Dataset[IdResVendorTransformerJob.Source.Vendor],
      Dataset[(VertexId, VertexId)],
      Dataset[IdResVendorTransformerJob.Source.UniqueVendor]
    )
  ): Dataset[(Long, Features)] = {
    import spark.implicits._
    val (vendors, connector, uniqueVendors) = {
      input
    }
    val vendorComparisons: RDD[Comparison] = {
      val comparators: RDD[(VertexId, Option[List[Comparator]])] = {
        vendors
          .map { vendor: IdResVendorTransformerJob.Source.Vendor =>
            vendor.model.uid -> Option(List(Comparator(vendor.model)))
          }
          .rdd
          .join(
            connector.rdd
          )
          .map {
            case (_, (vendor: Option[List[Comparator]], connectedId: VertexId)) =>
              connectedId -> vendor
          }
      }
      CandidateReducer(comparators)
        .flatMap { maybe =>
          Comparison.buildFromVendors(maybe.toList.flatten)
        }
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

    def apply[A, B](rdd: PairRDDFunctions[A, Option[List[B]]])(
      implicit spark: SparkSession
    ): RDD[Option[List[B]]] = {
      rdd
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

  // TODO: scale numEdgesInCommon
  // TODO: 0, 1, more than 1 -> categorical
  final case class Features(
    numTokens: Double,
    numTokensInCommon: Double,
    numSrcIds: Double,
    numSrcIdsInCommon: Double,
    sameCity: Double,
    sameZip: Double,
    sameState: Double
  ) {

    def toArray: Array[Double] = {
      Array(
        numTokens,
        numTokensInCommon,
        Features.scale(numSrcIds),
        Features.scale(numSrcIdsInCommon),
        sameCity,
        sameZip,
        sameState
      )
    }

  }

  object Features {

    def scale(raw: Double): Double = {
      if (raw <= 5) {
        raw
      } else {
        5
      }
    }

  }

  final case class Comparator(
    vendor: IdResVendorTransformerJob.Model
  ) {

    val nameTokens: Set[String] = {
      vendor.names.flatMap(cleanedNameTokens)
    }

    val addressTokens: Set[String] = {
      vendor.cities ++
        vendor.zip_codes
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
    right_side: Comparator,
    numSrcIds: Double,
    numSrcIdsInCommon: Double
  ) {

    lazy val features: Features = {
      Features(
        numTokens = numTokens,
        numTokensInCommon = numTokensInCommon,
        numSrcIds = numSrcIds,
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

    private lazy val sameCity: Boolean = {
      same(_.cities)
    }

    private lazy val sameZip: Boolean = {
      same(_.zip_codes)
    }

    private lazy val sameState: Boolean = {
      same(_.states)
    }

    private def toDouble(bool: Boolean): Double = {
      bool.compare(false)
    }

    private def same(f: IdResVendorTransformerJob.Model => Set[String]): Boolean = {
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

    def buildFromVendors(list: List[Comparator]): List[Comparison] = {
      val numSrcIds: Double = {
        list.flatMap(_.srcIdTokens).distinct.size.toDouble
      }
      combinations(list).map {
        case (left, right) =>
          Comparison(
            left_side = left,
            right_side = right,
            numSrcIds = numSrcIds,
            numSrcIdsInCommon = numSrcIds
          )
      }
    }

    def buildFromUniqueVendors(list: List[Comparator]): List[Comparison] = {
      combinations(list).map {
        case (left, right) =>
          val numSrcIds: Double = {
            left.vendor.src_ids.union(right.vendor.src_ids).size.toDouble
          }
          val numSrcIdsInCommon: Double = {
            left.vendor.src_ids.intersect(right.vendor.src_ids).size.toDouble
          }
          Comparison(
            left_side = left,
            right_side = right,
            numSrcIds = numSrcIds,
            numSrcIdsInCommon = numSrcIdsInCommon
          )
      }
    }

    private def combinations[A](list: List[A]): List[(A, A)] = {
      list.combinations(n = 2).toList.flatMap { combination: Seq[A] =>
        combination match {
          case left :: right :: Nil =>
            Some((left, right))
          case _ =>
            None
        }
      }
    }
  }

}
