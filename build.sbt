name := "hotspots"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.commons" % "commons-math3" % "3.0"
libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.6"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"
libraryDependencies += "it.unimi.dsi" % "fastutil" % "7.0.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0" % "provided"
parallelExecution in Test := false
