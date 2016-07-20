name := "hotspots"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "joda-time" % "joda-time" % "2.9.4"
libraryDependencies += "org.joda" % "joda-convert" % "1.8"
libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.6"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0" % "provided"
parallelExecution in Test := false

