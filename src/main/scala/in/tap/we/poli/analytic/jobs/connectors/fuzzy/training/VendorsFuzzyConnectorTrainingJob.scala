package in.tap.we.poli.analytic.jobs.connectors.fuzzy.training

import in.tap.base.spark.jobs.composite.OneInOneOutJob
import in.tap.base.spark.main.InArgs.OneInArgs
import in.tap.base.spark.main.OutArgs.OneOutArgs
import in.tap.we.poli.analytic.jobs.connectors.fuzzy.features.VendorsFuzzyConnectorFeaturesJob.Features
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe

class VendorsFuzzyConnectorTrainingJob(val inArgs: OneInArgs, val outArgs: OneOutArgs)(
  implicit
  val spark: SparkSession,
  val readTypeTagA: universe.TypeTag[(Long, Features)],
  val writeTypeTagA: universe.TypeTag[LabeledPoint]
) extends OneInOneOutJob[(Long, Features), LabeledPoint](inArgs, outArgs) {

  override def transform(input: Dataset[(Long, Features)]): Dataset[LabeledPoint] = {
    val features: Dataset[LabeledPoint] = {
      input.map {
        case (label: Long, features: Features) =>
          LabeledPoint(label = label.toDouble, features = Vectors.dense(features.toArray))
      }
    }
    val logReg = {
      new LogisticRegression()
    }
    val model: LogisticRegressionModel = {
      logReg.fit(features)
    }
    println(s"Intercept: ${model.intercept}")
    model.coefficients.toArray.foreach { coefficient: Double =>
      println(s"Coefficient: $coefficient")
    }
    features
  }

}
