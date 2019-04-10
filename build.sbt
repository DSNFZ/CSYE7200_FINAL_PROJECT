name := "finalproject"

version := "0.1"

scalaVersion := "2.11.9"

val scalaTestVersion = "2.2.4"

libraryDependencies += "org.scalatest" %% "scalatest" % scalaTestVersion % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.1"
libraryDependencies ++= Seq(
  "net.debasishg" %% "redisclient" % "3.9"
)

parallelExecution in Test := false