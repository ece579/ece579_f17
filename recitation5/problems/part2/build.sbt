// simple.sbt:
name := "PageRank"
version := "1.0"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % "2.2.0",
        "org.scala-lang" % "scala-library" % scalaVersion.value,
        "org.apache.spark" %% "spark-mllib" % "2.2.0",
        "org.apache.spark" %% "spark-graphx" % "2.2.0"
        )
fork in run := true
