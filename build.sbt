name := "RecBook"

version := "1.0"

lazy val `recbook` = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.8"

val sparkVersion = "1.6.1"

libraryDependencies ++= Seq(
  jdbc,
  cache,
  ws,
  specs2 % Test,
  // Spark
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "com.stratio.datasource" % "spark-mongodb_2.11" % "0.11.0",
  //Mongo driver for play 2.4.*
  "org.reactivemongo" %% "play2-reactivemongo" % "0.11.9",
  "org.mongodb.mongo-hadoop" % "mongo-hadoop-core" % "1.3.1",
  //Authentication lib
  "ws.securesocial" % "securesocial_2.11" % "3.0-M4",
  "net.codingwell" %% "scala-guice" % "4.0.0",
  "com.typesafe.play" %% "play-mailer" % "3.0.1",
  //Books Google Api
  "com.google.apis" % "google-api-services-books" % "v1-rev81-1.21.0",
  "org.apache.httpcomponents" % "httpasyncclient" % "4.1.1"
)

// Select which Hadoop version to use
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.4.1"

libraryDependencies += "org.mongodb" % "mongo-java-driver" % "3.2.2"

dependencyOverrides ++= Set(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
)

libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-log4j12")) }

unmanagedResourceDirectories in Test <+=  baseDirectory ( _ /"target/web/public/test" )

resolvers += Resolver.sonatypeRepo("releases")

resolvers += "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases"

routesImport ++= Seq("scala.language.reflectiveCalls", "controllers.SearchType._")