ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.16"

// Репозиторий Akka больше не нужен, Pekko доступен в Maven Central
resolvers ++= Seq(
  Resolver.mavenCentral
)

lazy val root = (project in file("."))
  .settings(
    name := "lesson-17"
  )

// Актуальная стабильная версия Pekko на данный момент
val PekkoVersion = "1.1.2"
val PekkoConnectorsKafkaVersion = "1.1.0"

libraryDependencies ++= Seq(
  "org.apache.pekko" %% "pekko-actor-typed"           % PekkoVersion,
  "org.apache.pekko" %% "pekko-stream"                % PekkoVersion,
  "org.apache.pekko" %% "pekko-connectors-kafka"      % PekkoConnectorsKafkaVersion,

  "ch.qos.logback"    % "logback-classic"             % "1.5.11",

  "org.apache.pekko" %% "pekko-actor-testkit-typed"   % PekkoVersion % Test,
  "org.scalatest"    %% "scalatest"                   % "3.2.19"     % Test
)