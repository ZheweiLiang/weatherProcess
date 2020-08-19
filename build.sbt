import scalariform.formatter.preferences._

scalaVersion in ThisBuild := "2.11.8"

name := "Spark Project With Scala"

libraryDependencies ++= Seq(
  "com.github.melrief" %% "pureconfig" % "0.6.0",
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-streaming" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.apache.spark" %% "spark-mllib" % "2.4.4",
  "io.argonaut" %% "argonaut" % "6.1",
  "org.json4s" %% "json4s-jackson" % "3.5.3",
  "org.json4s" %% "json4s-ext" % "3.5.3",
  "io.spray" %% "spray-json" % "1.3.2",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "ch.qos.logback" % "logback-core" % "1.2.3",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "ch.qos.logback.contrib" % "logback-json-classic" % "0.1.5",
  "ch.qos.logback.contrib" % "logback-jackson" % "0.1.5",
  "org.apache.hadoop" % "hadoop-aws" % "2.7.3",
  "org.scalacheck" %% "scalacheck" % "1.14.1" % "test"
)
