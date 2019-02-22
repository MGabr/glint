name := "Glint"

version := "0.2-SNAPSHOT"

organization := "at.mgabr"

scalaVersion := "2.11.8"
val scalaMajorMinorVersion = "2.11"

fork in Test := true

// Spark

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.6.5" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.5" % "provided"


// BLAS support

libraryDependencies += "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()

// Akka

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.20"

libraryDependencies += "com.typesafe.akka" %% "akka-remote" % "2.5.20"

libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.5.20"

libraryDependencies += "com.typesafe.akka" %% "akka-slf4j" % "2.5.20"


// Retry

resolvers += "softprops-maven" at "http://dl.bintray.com/content/softprops/maven"

libraryDependencies += "me.lessis" %% "retry" % "0.2.0"


// Breeze

libraryDependencies += "org.scalanlp" %% "breeze" % "0.13.2"

libraryDependencies += "org.scalanlp" %% "breeze-natives" % "0.13.2"


// Unit tests

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1" % "it,test"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "it,test"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.6.5" % "test" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.5" % "test" classifier "tests"


// Performance benchmarking

libraryDependencies += "com.storm-enroute" %% "scalameter" % "0.8.2" % "provided"


// Scala option parser

libraryDependencies += "com.github.scopt" %% "scopt" % "3.6.0"


// Logging, backend only as test dependency since the backend provided by spark is used otherwise

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"

libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.25" % "test"


// Resolvers

resolvers += Resolver.sonatypeRepo("public")

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"


// Set up scalameter

val scalaMeterFramework = new TestFramework("org.scalameter.ScalaMeterFramework")

testFrameworks += scalaMeterFramework

testOptions in ThisBuild += Tests.Argument(scalaMeterFramework, "-silent")

logBuffered := false


// Testing only sequential (due to binding to network ports)

parallelExecution in Test := false

// Lower Aeron buffer to prevent space on /dev/shm running out during local or CI tests

val aeronBufferLength = "-Daeron.term.buffer.length=1048576" // 1024 * 1024
javaOptions in Test += aeronBufferLength
javaOptions in IntegrationTest += aeronBufferLength

// Add it:assembly task to build separate jar containing only the integration test sources

Project.inConfig(IntegrationTest)(baseAssemblySettings)
assemblyJarName in (IntegrationTest, assembly) := s"${name.value}-it-assembly-${version.value}.jar"
test in (IntegrationTest, assembly) := {}
fullClasspath in (IntegrationTest, assembly) := {
  val cp = (fullClasspath in (IntegrationTest, assembly)).value
  cp.filter({ x => Seq("it-classes", "scalatest", "scalactic").exists(x.data.getPath.contains(_)) })
}

// Override it:test task to execute integration tests in Spark docker container

val sparkTestsMain = "glint.spark.Main"

import scala.sys.process._

test in IntegrationTest := {
  val startSparkTestEnv = "./spark-test-env.sh"
  val execSparkTests =
    s"""./spark-test-env.sh exec
        spark-submit
        --driver-java-options=$aeronBufferLength
        --conf spark.executor.extraJavaOptions=$aeronBufferLength
        --total-executor-cores 2
        --jars target/scala-$scalaMajorMinorVersion/${name.value}-assembly-${version.value}.jar
        --class $sparkTestsMain
        target/scala-$scalaMajorMinorVersion/${name.value}-it-assembly-${version.value}.jar
    """
  val stopSparkTestEnv = "./spark-test-env.sh stop"
  val rmSparkTestEnv = "./spark-test-env.sh rm"
  val exitCode = (startSparkTestEnv #&& execSparkTests #&& stopSparkTestEnv #&& rmSparkTestEnv !)
  if (exitCode != 0) {
    (stopSparkTestEnv ### rmSparkTestEnv !)
    throw new RuntimeException(s"Integration tests failed with nonzero exit value: $exitCode")
  }
}

test in IntegrationTest := (test in IntegrationTest).dependsOn(
  assembly,
  assembly in IntegrationTest
).value

// Add integration tests to sbt project

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)


// Scala documentation

scalacOptions in (Compile, doc) ++= Seq("-doc-root-content", baseDirectory.value+"/docs/root.txt")
scalacOptions in (Compile, doc) ++= Seq("-doc-title", "Glint")
scalacOptions in (Compile, doc) ++= Seq("-skip-packages", "akka")

enablePlugins(GhpagesPlugin)

git.remoteRepo := "git@github.com:MGabr/glint.git"

enablePlugins(SiteScaladocPlugin)

