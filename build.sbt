name := "spark-mongodb-connector"

version := "1.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.1.0"

libraryDependencies += "de.flapdoodle.embed" % "de.flapdoodle.embed.mongo" % "1.46.1"

libraryDependencies += "org.mongodb" %% "casbah" % "2.7.3"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"