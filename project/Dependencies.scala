import Dependencies.scalaTestVersion
import sbt.*
object Dependencies {
  private val mockitoScalaVersion = "1.17.30"
  private val pureConfigVersion = "0.17.5"
  private val daAwsClientVersion = "0.1.37-SNAPSHOT"
  private val scalaTestVersion = "3.2.18"

  lazy val awsDynamoDbClient = "uk.gov.nationalarchives" %% "da-dynamodb-client" % daAwsClientVersion
  lazy val awsEventBridgeClient = "uk.gov.nationalarchives" %% "da-eventbridge-client" % daAwsClientVersion
  lazy val logbackVersion = "2.22.1"
  lazy val dynamoFormatters = "uk.gov.nationalarchives" %% "dynamo-formatters" % "0.0.10-SNAPSHOT"
  lazy val log4jSlf4j = "org.apache.logging.log4j" % "log4j-slf4j2-impl" % logbackVersion
  lazy val log4jCore = "org.apache.logging.log4j" % "log4j-core" % logbackVersion
  lazy val log4jTemplateJson = "org.apache.logging.log4j" % "log4j-layout-template-json" % logbackVersion
  lazy val lambdaCore = "com.amazonaws" % "aws-lambda-java-core" % "1.2.3"
  lazy val lambdaJavaEvents = "com.amazonaws" % "aws-lambda-java-events" % "3.11.4"
  lazy val mockito = "org.scalatestplus" %% "mockito-5-10" % s"$scalaTestVersion.0"
  lazy val preservicaClient = "uk.gov.nationalarchives" %% "preservica-client-fs2" % "0.0.50-SNAPSHOT"
  lazy val pureConfigCats = "com.github.pureconfig" %% "pureconfig-cats-effect" % pureConfigVersion
  lazy val pureConfig = "com.github.pureconfig" %% "pureconfig-core" % pureConfigVersion
  lazy val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion
}
