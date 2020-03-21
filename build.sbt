name := "SparkKafkaStream"

version := "0.1"

scalaVersion := "2.11.11"

val sparkVersion = "2.3.2"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.2" % "provided"


libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % sparkVersion