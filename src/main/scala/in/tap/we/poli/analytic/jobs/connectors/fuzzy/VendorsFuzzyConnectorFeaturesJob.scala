package in.tap.we.poli.analytic.jobs.connectors.fuzzy

import in.tap.base.spark.jobs.composite.ThreeInOnOutJob
import in.tap.base.spark.main.InArgs.ThreeInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.connectors.cleanedNameTokens
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.VendorsFuzzyConnectorFeaturesJob._
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.VendorsFuzzyConnectorJob.CandidateGenerator
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.{Vendor, VendorLike}
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

// TODO: fix typo in parent class
class VendorsFuzzyConnectorFeaturesJob(val inArgs: ThreeInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[Vendor],
  val readTypeTagB: universe.TypeTag[(VertexId, VertexId)],
  val readTypeTagC: universe.TypeTag[UniqueVendor],
  val writeTypeTagA: universe.TypeTag[(Long, Features)]
) extends ThreeInOnOutJob[Vendor, (VertexId, VertexId), UniqueVendor, (Long, Features)](inArgs, outArgs) {

  override def transform(
    input: (Dataset[Vendor], Dataset[(VertexId, VertexId)], Dataset[UniqueVendor])
  ): Dataset[(Long, Features)] = {
    import spark.implicits._
    val (vendors: Dataset[Vendor], connector: Dataset[(VertexId, VertexId)], uniqueVendors: Dataset[UniqueVendor]) = {
      input
    }
    val vendorComparisons: RDD[VendorComparison] = {
      vendors
        .map { vendor: Vendor =>
          vendor.uid -> Option(Seq(vendor))
        }
        .rdd
        .join(
          connector.rdd
        )
        .map {
          case (_, (vendor: Option[Seq[Vendor]], connectedId: VertexId)) =>
            connectedId -> vendor
        }
        .reduceByKey(reduceCandidates)
        .flatMap {
          case (_, candidates: Option[Seq[Vendor]]) =>
            candidates match {
              case Some(c) => VendorComparison(c)
              case None    => Nil
            }
        }
    }
    val uniqueVendorComparisons: RDD[UniqueVendorComparison] = {
      CandidateGenerator(uniqueVendors)
    }
    val numPositives: Double = {
      vendorComparisons.count.toDouble
    }
    val numNegatives: Double = {
      uniqueVendorComparisons.count.toDouble
    }
    vendorComparisons
      .map { comparison: VendorComparison =>
        1L -> comparison.features
      }
      .sample(
        withReplacement = false,
        fraction = buildSamplingRatio(numPositives, numNegatives)
      )
      .union(
        uniqueVendorComparisons.map { comparison: UniqueVendorComparison =>
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

  def reduceCandidates[A](left: Option[Seq[A]], right: Option[Seq[A]]): Option[Seq[A]] = {
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

  // TODO: scale numEdgesInCommon
  // TODO: 0, 1, more than 1 -> categorical
  final case class Features(
    numTokens: Double,
    numTokensInCommon: Double,
    numEdgesInCommon: Double,
    sameCity: Double,
    sameZip: Double,
    sameState: Double
  ) {

    def toArray: Array[Double] = {
      Array(
        numTokens,
        numTokensInCommon,
        numEdgesInCommon,
        sameCity,
        sameZip,
        sameState
      )
    }

  }

  object Features {

    /**
     * Intended for use with [[Features.numEdgesInCommon]].
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

  /**
   * Used to build comparison
   * between two vendors that have been auto connected.
   *
   * @param left_side vendor
   * @param right_side vendor
   * @param numDistinctSrcIds num merged in connected component
   */
  final case class VendorComparison(
    left_side: Comparator[Vendor],
    right_side: Comparator[Vendor],
    numDistinctSrcIds: Double
  ) extends Comparison[Vendor] {

    override val numEdgesInCommon: Double = {
      Features.scale(numDistinctSrcIds)
    }

  }

  object VendorComparison {

    def apply(seq: Seq[Vendor]): Seq[VendorComparison] = {
      val numDistinctSrcIds: Double = {
        seq.map(_.edge.src_id).distinct.size.toDouble
      }
      seq.combinations(n = 2).toSeq.flatMap { combination: Seq[Vendor] =>
        combination match {
          case left :: right :: Nil =>
            Some(
              VendorComparison(
                left_side = Comparator(left),
                right_side = Comparator(right),
                numDistinctSrcIds = numDistinctSrcIds
              )
            )
          case _ => None
        }
      }
    }

  }

  /**
   * Used to build comparison
   * between two unique vendors that share CG set.
   *
   * @param left_side unique vendor
   * @param right_side unique vendor
   */
  final case class UniqueVendorComparison(
    left_side: Comparator[UniqueVendor],
    right_side: Comparator[UniqueVendor]
  ) extends Comparison[UniqueVendor] {

    override val numEdgesInCommon: Double = {
      val leftSrcIds: Set[VertexId] = {
        left_side.vendor.edges.map(_.src_id)
      }
      val rightSrcIds: Set[VertexId] = {
        right_side.vendor.edges.map(_.src_id)
      }
      val intersection = {
        leftSrcIds
          .intersect(rightSrcIds)
          .size
          .toDouble
      }
      Features.scale(intersection)
    }

  }

  object UniqueVendorComparison {

    def apply(seq: Seq[Comparator[UniqueVendor]]): Seq[UniqueVendorComparison] = {
      seq.combinations(n = 2).toSeq.flatMap { combination: Seq[Comparator[UniqueVendor]] =>
        combination match {
          case left :: right :: Nil => Some(UniqueVendorComparison(left, right))
          case _                    => None
        }
      }
    }

  }

  final case class Comparator[A <: VendorLike](
    vendor: A
  ) {

    val nameTokens: Set[String] = {
      cleanedNameTokens(vendor.name).toSet
    }

    val cgTokens: Set[String] = {
      nameTokens ++ Set(vendor.address.city, vendor.address.zip_code).flatten
    }

  }

  trait Comparison[A <: VendorLike] {

    val left_side: Comparator[A]

    val right_side: Comparator[A]

    val numEdgesInCommon: Double

    lazy val features: Features = {
      Features(
        numTokens,
        numTokensInCommon,
        numEdgesInCommon,
        toDouble(sameCity),
        toDouble(sameZip),
        toDouble(sameState)
      )
    }

    private lazy val numTokens: Double = {
      Seq(left_side.nameTokens.size, right_side.nameTokens.size).max.toDouble
    }

    // TODO: Validate for city/state
    private lazy val numTokensInCommon: Double = {
      left_side.nameTokens.intersect(right_side.nameTokens).size.toDouble
    }

    private lazy val sameCity: Option[Boolean] = {
      same((u: VendorLike) => u.address.city)
    }

    private lazy val sameZip: Option[Boolean] = {
      same((u: VendorLike) => u.address.zip_code)
    }

    private lazy val sameState: Option[Boolean] = {
      same((u: VendorLike) => u.address.state)
    }

    private def toDouble(maybeBool: Option[Boolean]): Double = {
      maybeBool.map(_.compare(false)).getOrElse(0).toDouble
    }

    private def same(f: VendorLike => Option[String]): Option[Boolean] = {
      for {
        leftV <- f(left_side.vendor).map(_.toLowerCase)
        rightV <- f(right_side.vendor).map(_.toLowerCase)
      } yield {
        leftV.equals(rightV)
      }
    }

  }

}
