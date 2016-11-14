lazy val commonSettings = Seq(
  version := "0.1-SNAPSHOT",
  organization := "uk.gov.ons",
  scalaVersion := "2.10.6",
  test in assembly := {}
)

lazy val `address-index-batch` = project.in(file("batch")).
  settings(commonSettings: _*).
  settings(
    mainClass in assembly := Some("uk.gov.ons.addressindex.Main"),
    name := "ons-ai-batch",
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )
