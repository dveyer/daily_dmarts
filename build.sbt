import sbt.Keys.{exportJars, scalaVersion}
import sbtassembly.MergeStrategy

name := "daily_dmarts"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.1.0"

mainClass in Compile := Some("MainApp")
mainClass in (Compile, packageBin) := Some("MainApp")

mainClass in assembly := Some("MainApp")
assemblyJarName in assembly := "daily_dmarts.jar"

resolvers += Resolver.url("bintray-sbt-plugins", url("http://dl.bintray.com/sbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.last
}

        