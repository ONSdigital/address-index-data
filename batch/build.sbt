resolvers ++= Seq(
  // allows us to include spark packages
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/",
  "conjars" at "http://conjars.org/repo"
)

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-hive" % "2.4.0" % "provided",
  "commons-httpclient" %% "commons-httpclient" % "3.1",
  "com.databricks" %% "spark-csv" % "1.5.0",
  "com.typesafe" % "config" % "1.3.3",
  "org.elasticsearch" %% "elasticsearch-spark-20" % "7.3.1"  excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "org.rogach" %% "scallop" % "3.1.5",
  "org.scalaj" %% "scalaj-http" % "2.4.1",
  "com.crealytics" %% "spark-excel" % "0.10.2"

)

dependencyOverrides += "commons-codec" % "commons-codec" % "1.11"

scalacOptions ++= List("-unchecked", "-Xlint")