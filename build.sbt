name := "project-2"

version := "0.1"

scalaVersion := "2.12.6"

libraryDependencies ++=Seq( 
  "org.apache.spark" %% "spark-core" % "3.0.1", 
  "org.apache.spark" %% "spark-sql" % "3.0.1",
  "org.apache.spark" %% "spark-mllib" % "3.0.1",
  "org.apache.spark" %% "spark-streaming" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)