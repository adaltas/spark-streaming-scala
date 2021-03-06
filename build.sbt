name := "taxi-streaming-scala"
scalaVersion := "2.11.12"

fork in Test := true

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.1"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.5.0"
libraryDependencies += "org.apache.spark" %% "spark-hive-thriftserver" % "2.4.1"