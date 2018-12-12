resolvers ++= Seq(
  // allows us to include spark packages
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/",
  "conjars" at "http://conjars.org/repo"
)

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "org.apache.spark" %% "spark-core" % "2.2.0",
  "org.apache.spark" %% "spark-sql" % "2.2.0",
  "org.apache.spark" %% "spark-hive" % "2.2.0",
  "com.databricks" %% "spark-csv" % "1.5.0",
  "com.typesafe" % "config" % "1.3.1",
  "org.elasticsearch" %% "elasticsearch-spark-20" % "5.6.3"  excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "org.rogach" %% "scallop" % "3.0.3",
  "org.scalaj" %% "scalaj-http" % "2.3.0",
  "com.crealytics" %% "spark-excel" % "0.10.2"

)

scalacOptions ++= List("-unchecked", "-Xlint")