package in.tap.we.poli.analytic.jobs

import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

trait BaseSparkJobSpec extends FlatSpec with Matchers {

  implicit val spark: SparkSession = {
    SparkSession
      .builder()
      .appName("Test")
      .master("local[*]")
      .getOrCreate()
  }

}
