name := "Spark AI Test"

version := "0.1"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq("org.scalatest" %% "scalatest" % "3.0.0" % "test",
"com.novocode" % "junit-interface" % "0.9" % "test")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-streaming" % "2.6.4"

assemblyJarName in assembly := s"${name.value.replace(' ','-')}-${version.value}.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
