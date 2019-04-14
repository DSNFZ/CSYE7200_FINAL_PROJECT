name := "finalproject"

version := "0.1"

scalaVersion := "2.11.9"

val scalaTestVersion = "2.2.4"

libraryDependencies += "org.scalatest" %% "scalatest" % scalaTestVersion % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.1"
//libraryDependencies +="com.typessafe.play"%% "play" % "2.7.0"
//libraryDependencies += "org.scala-lang" % "scala-actors" % "2.11.7"
//libraryDependencies ++= Seq(
//  "com.typesafe.akka" %% "akka-actor" % "2.5.22",
//  "com.typesafe.akka" %% "akka-testkit" % "2.5.22" % Test
//)
parallelExecution in Test := false