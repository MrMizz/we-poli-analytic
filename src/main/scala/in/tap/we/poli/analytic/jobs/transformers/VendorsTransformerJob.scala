package in.tap.we.poli.analytic.jobs.transformers

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.transformers.VendorsTransformerJob._
import in.tap.we.poli.models.OperatingExpenditures
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class VendorsTransformerJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[OperatingExpenditures],
  val writeTypeTagA: universe.TypeTag[Vendor]
) extends OneInOneOutJob[OperatingExpenditures, Vendor](inArgs, outArgs) {

  override def transform(input: Dataset[OperatingExpenditures]): Dataset[Vendor] = {
    input.flatMap(Vendor.fromOperatingExpenditures)
  }

}

object VendorsTransformerJob {

  import in.tap.we.poli.analytic.jobs.graph.edges.CommitteeToVendorEdgeJob.ExpenditureEdge

  val STOP_WORDS: Set[String] = {
    Set("ltd", "llc", "inc")
  }

  val PUNCTUATION_REGEX: String = {
    """\p{P}"""
  }

  final case class Vendor(
    uid: Long,
    name: String,
    city: Option[String],
    state: Option[String],
    zip_code: Option[String],
    memo: Option[String],
    edge: ExpenditureEdge
  ) {

    lazy val hash1: Option[String] = {
      for {
        city <- city.map(_.toLowerCase)
        state <- state.map(_.toLowerCase)
      } yield {
        s"${name.toLowerCase}_${city}_$state"
      }
    }

    lazy val hash2: Option[String] = {
      for {
        city <- city.map(_.toLowerCase)
        state <- state.map(_.toLowerCase)
      } yield {
        s"${cleanedName}_${city}_$state"
      }
    }

    lazy val hash3: Option[String] = {
      for {
        zip <- zip_code.map(_.toLowerCase.take(5))
      } yield {
        s"${cleanedName}_$zip"
      }
    }

    lazy val hashes: Seq[String] = {
      Seq(
        hash1,
        hash2,
        hash3
      ).flatten
    }

    private lazy val cleanedName: String = {
      name.toLowerCase // to lower case
        .replaceAll(PUNCTUATION_REGEX, "") // strip punctuation
        .filterNot { char: Char =>
          java.lang.Character.isDigit(char) // filter numeric
        }
        .split(" ") // tokenize
        .filterNot { char: String =>
          STOP_WORDS.contains(char) // filter stop words
        }
        .fold("")(_ + _) // join
    }

  }

  object Vendor {

    def fromOperatingExpenditures(operatingExpenditures: OperatingExpenditures): Option[Vendor] = {
      for {
        sub_id <- operatingExpenditures.SUB_ID
        name <- operatingExpenditures.NAME
      } yield {
        Vendor(
          uid = sub_id,
          name = name,
          city = operatingExpenditures.CITY,
          state = operatingExpenditures.STATE,
          zip_code = operatingExpenditures.ZIP_CODE,
          memo = operatingExpenditures.PURPOSE,
          edge = ExpenditureEdge.fromOperatingExpenditures(operatingExpenditures)
        )
      }
    }

  }

}
