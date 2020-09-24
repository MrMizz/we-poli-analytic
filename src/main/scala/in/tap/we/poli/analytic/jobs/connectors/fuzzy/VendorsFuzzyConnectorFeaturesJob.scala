package in.tap.we.poli.analytic.jobs.connectors.fuzzy

import in.tap.base.spark.jobs.composite.ThreeInOnOutJob
import in.tap.base.spark.main.InArgs.ThreeInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.connectors.ConnectorUtils
import in.tap.we.poli.analytic.jobs.mergers.VendorsMergerJob.UniqueVendor
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob.{Vendor, VendorLike}
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

// TODO: fix typo in parent class
class VendorsFuzzyConnectorFeaturesJob(val inArgs: ThreeInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[Vendor],
  val readTypeTagB: universe.TypeTag[(VertexId, VertexId)],
  val readTypeTagC: universe.TypeTag[UniqueVendor],
  val writeTypeTagA: universe.TypeTag[(VertexId, VertexId)]
) extends ThreeInOnOutJob[Vendor, (VertexId, VertexId), UniqueVendor, (VertexId, VertexId)](inArgs, outArgs) {

  override def transform(
    input: (Dataset[Vendor], Dataset[(VertexId, VertexId)], Dataset[UniqueVendor])
  ): Dataset[(VertexId, VertexId)] = {
    ???
  }

}

object VendorsFuzzyConnectorFeaturesJob {

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

    override def numEdges: Double = {
      numMerged
    }

    override def numEdgesInCommon: Double = {
      numMerged
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

  final case class Comparator[A <: VendorLike](
    vendor: A
  ) {

    val nameTokens: Set[String] = {
      ConnectorUtils.cleanedNameTokens(vendor.name).toSet
    }

  }

  trait Comparison[A <: VendorLike] {

    def left_side: Comparator[A]

    def right_side: Comparator[A]

    def numEdges: Double

    def numEdgesInCommon: Double

    val features: Features = {
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
