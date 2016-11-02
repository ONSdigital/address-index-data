name := "Address Index Batch‘"

version := "0.1.0"

scalaVersion := "2.10.6"

resolvers ++= Seq(
  // allows us to include spark packages
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/",
  "conjars" at "http://conjars.org/repo"
)



libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-sql" % "1.6.0",
  "com.databricks" %% "spark-csv" % "1.5.0",
  "com.typesafe" % "config" % "1.3.1",
  "org.elasticsearch" %% "elasticsearch-spark" % "2.4.0",
  "org.scalatest" %% "scalatest" % "3.0.0" % Test
)

scalacOptions ++= List("-unchecked", "-Xlint")