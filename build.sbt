lazy val commonSettings = Seq(
  version := "0.0.1",
  organization := "uk.gov.ons",
  scalaVersion := "2.12.14",
  assembly / test := {}
)

lazy val buildSettings = Seq(
  assembly / mainClass := Some("uk.gov.ons.addressindex.Main"),
  name := "ons-ai-batch",
  assembly / assemblyMergeStrategy := {
    case "reference.conf" => MergeStrategy.concat
    case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
    case PathList("META-INF", _*) => MergeStrategy.discard
    case _ => MergeStrategy.first
  }
)

lazy val `address-index-batch` = project.in(file("batch")).settings(commonSettings ++ buildSettings: _*)
