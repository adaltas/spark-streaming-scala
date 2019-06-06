name := "taxi-streaming-scala"
scalaVersion := "2.11.12"

fork in Test := true

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.1"

