resolvers ++= Seq(
  // allows us to include spark packages
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/",
  "conjars" at "https://conjars.org/repo"
)

val localTarget: Boolean = true
// set to true when testing locally, false for deployment to Cloudera
// reload all sbt projects to clear ivy cache

val localDeps = Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" %% "spark-hive" % "2.4.0"
)

val clouderaDeps = Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-hive" % "2.4.0" % "provided",
  "commons-httpclient" % "commons-httpclient" % "3.1"
)

val otherDeps = Seq(
  "com.databricks" %% "spark-csv" % "1.5.0",
  "com.typesafe" % "config" % "1.3.3",
  "org.elasticsearch" %% "elasticsearch-spark-20" % "7.3.1"  excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "org.rogach" %% "scallop" % "3.1.5",
  "org.scalaj" %% "scalaj-http" % "2.4.1",
  "com.crealytics" %% "spark-excel" % "0.10.2"
)

if (localTarget) libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  "org.scala-lang" % "scala-reflect" % scalaVersion.value
) ++ localDeps ++ otherDeps
else libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  "org.scala-lang" % "scala-reflect" % scalaVersion.value
) ++ clouderaDeps ++ otherDeps

dependencyOverrides += "commons-codec" % "commons-codec" % "1.11"

scalacOptions ++= List("-unchecked", "-Xlint")