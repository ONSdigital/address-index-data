resolvers ++= Seq(
  // allows us to include spark packages
  "spark-packages" at "https://repos.spark-packages.org/",
  "conjars" at "https://conjars.org/repo"
)

val localTarget: Boolean = false
// set to true when testing locally (or to build a fat jar)
// false for deployment to Cloudera with a thin jar
// reload all sbt projects to clear ivy cache

val localDeps = Seq(
  "org.apache.spark" %% "spark-core" % "3.1.2",
  "org.apache.spark" %% "spark-sql" % "3.1.2",
  "org.apache.spark" %% "spark-hive" % "3.1.2"
)

val clouderaDeps = Seq(
  "org.apache.spark" %% "spark-core" % "3.1.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.1.2" % "provided",
  "org.apache.spark" %% "spark-hive" % "3.1.2" % "provided",
  "org.apache.httpcomponents" % "httpclient" % "4.5.13"
)

val otherDeps = Seq(
 // "com.databricks" % "spark-csv_2.11" % "1.5.0",
  "com.typesafe" % "config" % "1.4.1",
  "org.elasticsearch" %% "elasticsearch-spark-30" % "7.12.0" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.rogach" %% "scallop" % "4.0.3",
  "org.scalaj" %% "scalaj-http" % "2.4.2",
  "com.crealytics" %% "spark-excel" % "0.14.0",
  "org.scalatest" %% "scalatest" % "3.2.9" % Test
)

if (localTarget) libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  "org.scala-lang" % "scala-reflect" % scalaVersion.value
) ++ localDeps ++ otherDeps
else libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  "org.scala-lang" % "scala-reflect" % scalaVersion.value
) ++ clouderaDeps ++ otherDeps

dependencyOverrides += "commons-codec" % "commons-codec" % "1.15"

scalacOptions ++= List("-unchecked", "-Xlint")