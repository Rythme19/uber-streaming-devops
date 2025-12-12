ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.18"

lazy val root = (project in file("."))
  .settings(
    name := "uber-streaming-devops",
    organization := "com.example",

    libraryDependencies ++= Seq(
      // Spark (provided by Spark runtime)
      "org.apache.spark" %% "spark-core" % "4.0.1" % "provided",
      "org.apache.spark" %% "spark-sql" % "4.0.1" % "provided",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "4.0.1" % "provided",

      // Kafka client — MUST be included in JAR (not "provided")
      "org.apache.kafka" % "kafka-clients" % "3.8.0"
    ),

    // Assembly settings
    assembly / assemblyJarName := s"${name.value}-assembly-${version.value}.jar",
    assembly / test := {},
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )