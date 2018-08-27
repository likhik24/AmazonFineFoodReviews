name := "AmazonFineFoods"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.0.0"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/",
  "SparkPackages" at "https://dl.bintray.com/spark-packages/maven/"
)


licenses := Seq("GPL-3.0" -> url("http://opensource.org/licenses/GPL-3.0"))
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",

"edu.stanford.nlp" % "stanford-corenlp" % "3.7.0",
"com.google.protobuf" % "protobuf-java" % "3.2.0",
"edu.stanford.nlp" % "stanford-corenlp" % "3.7.0" % "test" classifier "models",
"org.scalatest" %% "scalatest" % "3.0.1" % "test"
)
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.7.0" withSources() withJavadoc()
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.7.0" classifier "models"
libraryDependencies += "databricks" % "spark-corenlp" % "0.2.0-s_2.11"


