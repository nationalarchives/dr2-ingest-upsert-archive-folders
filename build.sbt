import Dependencies.*
import uk.gov.nationalarchives.sbt.Log4j2MergePlugin.log4j2MergeStrategy

ThisBuild / organization := "uk.gov.nationalarchives"
ThisBuild / scalaVersion := "3.3.1"

lazy val root = (project in file(".")).settings(
  name := "dr2-ingest-upsert-archive-folders",
  libraryDependencies ++= Seq(
    awsDynamoDbClient,
    awsEventBridgeClient,
    log4jSlf4j,
    log4jCore,
    log4jTemplateJson,
    lambdaCore,
    lambdaJavaEvents,
    mockito % Test,
    preservicaClient,
    pureConfig,
    dynamoFormatters,
    pureConfigCats,
    scalaTest % Test
  ),
  scalacOptions ++= Seq("-Wunused:imports", "-Werror", "-new-syntax", "-rewrite")
)

(assembly / assemblyJarName) := "dr2-ingest-upsert-archive-folders.jar"

(assembly / assemblyMergeStrategy) := {
  case PathList(ps @ _*) if ps.last == "Log4j2Plugins.dat" => log4j2MergeStrategy
  case _                                                   => MergeStrategy.first
}

(Test / fork) := true
(Test / envVars) := Map("AWS_LAMBDA_FUNCTION_NAME" -> "testfunction")
