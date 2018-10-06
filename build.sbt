name := "PredictionIO_EventSender"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-sql_2.11" % "2.3.1",
  "org.apache.spark" % "spark-core_2.11" % "2.3.1",
  "org.apache.spark" % "spark-streaming_2.11" % "2.3.1",
  "com.google.code.gson" % "gson" % "1.7.1",
  "org.apache.predictionio" % "predictionio-sdk-java-client" % "0.13.0"
)
