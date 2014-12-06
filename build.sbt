name := "spark-mongodb-connector"

version := "0.1-SNAPSHOT"

organization := "com.github.spirom"

scalaVersion := "2.10.4"

publishMavenStyle := true

// core dependencies

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.1.0"

libraryDependencies += "org.mongodb" %% "casbah" % "2.7.3"

// testing stuff below here

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

libraryDependencies += "de.flapdoodle.embed" % "de.flapdoodle.embed.mongo" % "1.46.1" % "test"

