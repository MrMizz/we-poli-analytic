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

  val STOP_WORDS: Set[String] = {
    Set("ltd", "llc", "inc")
  }

  val PUNCTUATION_REGEX: String = """\p{P}"""

  final case class Vendor(
    uid: Long,
    name: Option[String],
    city: Option[String],
    state: Option[String],
    zip_code: Option[String]
  ) {

    lazy val hash1: Option[String] = {
      for {
        name <- name.map(_.toLowerCase)
        city <- city.map(_.toLowerCase)
        state <- state.map(_.toLowerCase)
      } yield {
        s"${name}_${city}_$state"
      }
    }

    lazy val hash2: Option[String] = {
      for {
        name <- cleanedName
        city <- city.map(_.toLowerCase)
        state <- state.map(_.toLowerCase)
      } yield {
        s"${name}_${city}_$state"
      }
    }

    lazy val hash3: Option[String] = {
      for {
        name <- cleanedName
        zip <- zip_code.map(_.toLowerCase.take(5))
      } yield {
        s"${name}_${zip}"
      }
    }

    lazy val hashes: Seq[String] = {
      Seq(
        hash1,
        hash2,
        hash3
      ).flatten
    }

    private lazy val cleanedName: Option[String] = {
      name.map { n: String =>
        n.toLowerCase // to lower case
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

  }

  object Vendor {

    def fromOperatingExpenditures(operatingExpenditures: OperatingExpenditures): Option[Vendor] = {
      for {
        sub_id <- operatingExpenditures.SUB_ID
      } yield {
        Vendor(
          uid = sub_id,
          name = operatingExpenditures.NAME,
          city = operatingExpenditures.CITY,
          state = operatingExpenditures.STATE,
          zip_code = operatingExpenditures.ZIP_CODE
        )
      }
    }

  }

}
