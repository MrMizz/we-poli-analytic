package in.tap.we.poli.analytic.jobs.connectors.fuzzy

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class VendorsFuzzyConnectorTrainingJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[LabeledPoint],
  val writeTypeTagA: universe.TypeTag[Boolean]
) extends OneInOneOutJob[LabeledPoint, Boolean](inArgs, outArgs) {

  override def transform(input: Dataset[LabeledPoint]): Dataset[Boolean] = {
    val logReg = {
      new LogisticRegression()
    }
    val model: LogisticRegressionModel = {
      logReg.fit(input)
    }
    println(s"Intercept: ${model.intercept}")
    model.coefficients.toArray.foreach { coefficient: Double =>
      println(s"Coefficient: $coefficient")
    }
    import spark.implicits._
    Seq(true).toDS
  }

}
