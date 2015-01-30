name := "spark-mongodb-connector"

version := "0.4.0-SNAPSHOT"

organization := "com.github.spirom"

scalaVersion := "2.10.4"

publishMavenStyle := true

publishArtifact in Test := false

pomIncludeRepository := { x => false }

// core dependencies

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.2.0"

libraryDependencies += "org.mongodb" %% "casbah" % "2.7.3"

// testing stuff below here

libraryDependencies += "org.scalatest" %% "scalatest" % "2.0" % "test"

libraryDependencies += "de.flapdoodle.embed" % "de.flapdoodle.embed.mongo" % "1.46.1" % "test"

publishTo <<= version { v: String =>
  val nexus = "https://oss.sonatype.org/"
  if (v.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

pomExtra :=
  <url>http://github.com/spirom/spark-mongodb-connector</url>
  <licenses>
    <license>
      <name>Apache 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:spirom/spark-mongodb-connector.git</url>
    <connection>scm:git:git@github.com:spirom/spark-mongodb-connector.git</connection>
  </scm>
  <developers>
    <developer>
      <id>spirom</id>
      <name>Spiro Michaylov</name>
      <url>https://github.com/spirom</url>
    </developer>
  </developers>

