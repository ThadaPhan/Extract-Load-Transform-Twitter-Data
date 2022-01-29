ThisBuild / version := "0.2.0"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "Sentiment_Analysis"
  )

libraryDependencies ++= Seq(
  "edu.stanford.nlp" % "stanford-corenlp" % "3.5.0",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.5.0" classifier "models",
  "org.apache.spark" %% "spark-core" % "3.0.3",
  "org.apache.spark" %% "spark-sql" % "3.0.3",
  "org.apache.spark" %% "spark-hive" % "3.0.3",
  "mysql" % "mysql-connector-java" % "8.0.26",
  "com.github.nscala-time" %% "nscala-time" % "2.30.0"

)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}