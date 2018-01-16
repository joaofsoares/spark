name := "spark-project"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.2.1",
  "org.apache.spark" %% "spark-sql" % "2.2.1",
  "org.apache.spark" %% "spark-streaming" % "2.2.1" % "provided",
  "mysql" % "mysql-connector-java" % "6.0.6")
