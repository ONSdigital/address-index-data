lazy val commonSettings = Seq(
  version := version.value,
  organization := "uk.gov.ons",
  scalaVersion := "2.11.8",
  test in assembly := {}
)

lazy val buildSettings = Seq(
  mainClass in assembly := Some("uk.gov.ons.addressindex.Main"),
  name := "ons-ai-batch",
  assemblyMergeStrategy in assembly := {
    case "reference.conf" => MergeStrategy.concat
    case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
    case PathList("META-INF", ps @ _*) => MergeStrategy.discard
    case _ => MergeStrategy.first
  }
)

lazy val `address-index-batch` = project.in(file("batch")).settings(commonSettings ++ buildSettings: _*)
