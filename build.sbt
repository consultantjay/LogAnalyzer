name := "LogAnalyzer"
version := "0.1"
scalaVersion := "2.11.11"
/*libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0"*/
libraryDependencies ++= Seq("org.apache.spark" % "spark-sql_2.11" % "2.2.0",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.2.0",
  "org.apache.kafka" % "kafka-clients" % "0.11.0.1")