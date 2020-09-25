package in.tap.we.poli.analytic.jobs.connectors.fuzzy

import in.tap.base.spark.jobs.composite.ThreeInOnOutJob
import in.tap.base.spark.main.InArgs.ThreeInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.connectors.ConnectorUtils
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.VendorsFuzzyConnectorFeaturesJob.{
  reduceCandidates, Comparator, Features, UniqueVendorComparison, VendorComparison
}
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.{Vendor, VendorLike}
import org.apache.spark.graphx.VertexId
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

// TODO: fix typo in parent class
//  Sampling
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
      uniqueVendors
        .flatMap { uniqueVendor: UniqueVendor =>
          val comparator: Comparator[UniqueVendor] = Comparator(uniqueVendor)
          val candidate: Option[Seq[Comparator[UniqueVendor]]] = Option(Seq(comparator))
          comparator.nameTokens.map { token: String =>
            token -> candidate
          }
        }
        .rdd
        .reduceByKey(reduceCandidates)
        .flatMap {
          case (_, candidates: Option[Seq[Comparator[UniqueVendor]]]) =>
            candidates match {
              case Some(c) => UniqueVendorComparison(c)
              case None    => Nil
            }
        }
    }
    vendorComparisons
      .map { comparison: VendorComparison =>
        1L -> comparison.features
      }
      .union(
        uniqueVendorComparisons.map { comparison: UniqueVendorComparison =>
          0L -> comparison.features
        }
      )
      .toDS
  }

}

object VendorsFuzzyConnectorFeaturesJob {

  val MAX_COMPARISON_SIZE: Int = {
    100
  }

  def reduceCandidates[A](left: Option[Seq[A]], right: Option[Seq[A]]): Option[Seq[A]] = {
    (left, right) match {
      case (None, _) => None
      case (_, None) => None
      case (Some(l), Some(r)) =>
        if ((l.size + r.size) <= MAX_COMPARISON_SIZE) {
          Some(l ++ r)
        } else {
          None
        }
    }
  }

  final case class Features(
    numTokens: Double,
    numTokensInCommon: Double,
    numEdges: Double,
    numEdgesInCommon: Double,
    sameCity: Double,
    sameZip: Double,
    sameState: Double
  ) {

    def toArray: Array[Double] = {
      Array(
        numTokens,
        numTokensInCommon,
        numEdges,
        numEdgesInCommon,
        sameCity,
        sameZip,
        sameState
      )
    }

  }

  /**
   * Used to build comparison
   * between two vendors that have been auto connected.
   *
   * @param left_side vendor
   * @param right_side vendor
   * @param numMerged num merged in connected component
   */
  final case class VendorComparison(
    left_side: Comparator[Vendor],
    right_side: Comparator[Vendor],
    numMerged: Double
  ) extends Comparison[Vendor] {

    override val numEdges: Double = {
      numMerged
    }

    override val numEdgesInCommon: Double = {
      numMerged
    }

  }

  object VendorComparison {

    def apply(seq: Seq[Vendor]): Seq[VendorComparison] = {
      val numMerged: Int = seq.size
      seq.combinations(n = 2).toSeq.flatMap { combination: Seq[Vendor] =>
        combination match {
          case left :: right :: Nil =>
            Some(
              VendorComparison(
                left_side = Comparator(left),
                right_side = Comparator(right),
                numMerged = numMerged
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

    override val (numEdges, numEdgesInCommon): (Double, Double) = {
      val leftSrcIds: Set[VertexId] = {
        left_side.vendor.edges.map(_.src_id)
      }
      val rightSrcIds: Set[VertexId] = {
        right_side.vendor.edges.map(_.src_id)
      }
      Seq(
        leftSrcIds.size,
        rightSrcIds.size
      ).max.toDouble -> leftSrcIds
        .intersect(rightSrcIds)
        .size
        .toDouble
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
      ConnectorUtils.cleanedNameTokens(vendor.name).toSet
    }

  }

  trait Comparison[A <: VendorLike] {

    val left_side: Comparator[A]

    val right_side: Comparator[A]

    val numEdges: Double

    val numEdgesInCommon: Double

    lazy val features: Features = {
      Features(
        numTokens,
        numTokensInCommon,
        numEdges,
        numEdgesInCommon,
        toDouble(sameCity),
        toDouble(sameZip),
        toDouble(sameState)
      )
    }

    private lazy val numTokens: Double = {
      Seq(left_side.nameTokens.size, right_side.nameTokens.size).max.toDouble
    }

    private lazy val numTokensInCommon: Double = {
      left_side.nameTokens.intersect(right_side.nameTokens).size.toDouble
    }

    private lazy val sameCity: Option[Boolean] = {
      same((u: VendorLike) => u.city)
    }

    private lazy val sameZip: Option[Boolean] = {
      same((u: VendorLike) => u.zip_code)
    }

    private lazy val sameState: Option[Boolean] = {
      same((u: VendorLike) => u.state)
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
