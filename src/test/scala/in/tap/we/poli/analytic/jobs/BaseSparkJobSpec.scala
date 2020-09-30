package in.tap.we.poli.analytic.jobs

import org.apache.spark.sql.SparkSession

trait BaseSparkJobSpec extends BaseSpec {

  implicit val spark: SparkSession = {
    SparkSession
      .builder()
      .appName("Test")
      .master("local[*]")
      .getOrCreate()
  }

}
