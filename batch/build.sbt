name := "Address Index Batch‘"

version := "0.0.0"

scalaVersion := "2.10.6"

// allows us to include spark packages
resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.2",
  "org.apache.spark" %% "spark-sql" % "1.6.2",
  "com.databricks" %% "spark-csv" % "1.5.0",
  "com.typesafe" % "config" % "1.3.1",
  "org.scalatest" %% "scalatest" % "3.0.0" % Test
)

scalacOptions ++= List("-unchecked", "-Xlint")