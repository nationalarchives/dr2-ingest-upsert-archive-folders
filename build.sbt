import Dependencies._
import uk.gov.nationalarchives.sbt.Log4j2MergePlugin.log4j2MergeStrategy

ThisBuild / organization := "uk.gov.nationalarchives"
ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file(".")).
  settings(
    name := "dr2-ingest-upsert-archive-folders",
    libraryDependencies ++= Seq(
      awsDynamoDbClient,
      awsEventBridgeClient,
      log4jSlf4j,
      log4jCore,
      log4jTemplateJson,
      lambdaCore,
      lambdaJavaEvents,
      mockitoScala,
      preservicaClient,
      pureConfig,
      pureConfigCats,
      scalaTest % Test
    )
  )

(assembly / assemblyJarName) := "dr2-ingest-upsert-archive-folders.jar"

scalacOptions ++= Seq("-Wunused:imports", "-Werror")

(assembly / assemblyMergeStrategy) := {
  case PathList(ps @ _*) if ps.last == "Log4j2Plugins.dat" => log4j2MergeStrategy
  case _                                                   => MergeStrategy.first
}
