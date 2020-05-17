name := "we-poli-analytic"
organization := "in.tap"
version := "1.0.0-SNAPSHOT"
description := "Transformations & Analytics of FEC Data."

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
publishMavenStyle := true

scalaVersion := "2.11.12"

val versionSpark: String = "2.4.0"

libraryDependencies ++= Seq(
  // Spark
  "in.tap" %% "spark-base" % "1.0.0-SNAPSHOT",
  // Models
  "in.tap" %% "we-poli-models" % "1.0.0-SNAPSHOT",
  // apache spark
  "org.apache.spark" %% "spark-core" % versionSpark,
  "org.apache.spark" %% "spark-sql" % versionSpark,
  // Testing
  "org.scalatest" %% "scalatest" % "3.1.1" % Test
)

mainClass in assembly := Some("in.tap.we.poli.analytic.Main")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _                             => MergeStrategy.first
}
