name := "SparkDemo"

version := "1.0"

scalaVersion := "2.12.10"


libraryDependencies ++= Seq(
  // https://mvnrepository.com/artifact/org.apache.spark/spark-core
  "org.apache.spark" %% "spark-core" % "3.1.2",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
  "org.apache.spark" %% "spark-sql" % "3.1.2"
)