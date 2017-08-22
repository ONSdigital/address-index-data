resolvers ++= Seq(
  // allows us to include spark packages
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/",
  "conjars" at "http://conjars.org/repo"
)

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "org.apache.spark" %% "spark-core" % "1.6.3",
  "org.apache.spark" %% "spark-sql" % "1.6.3",
  "org.apache.spark" %% "spark-hive" % "1.6.3",
  "com.databricks" %% "spark-csv" % "1.5.0",
  "com.typesafe" % "config" % "1.3.1",
  "org.elasticsearch" %% "elasticsearch-spark" % "2.4.0"  excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.scalatest" %% "scalatest" % "3.0.0" % Test,
  "org.rogach" %% "scallop" % "2.0.5",
  "org.scalaj" %% "scalaj-http" % "2.3.0"
)

scalacOptions ++= List("-unchecked", "-Xlint")