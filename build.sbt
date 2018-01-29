name := "BigDataUniversity"
 
version := "1.1.4"

val sparkVersion = "1.6.0"
 
scalaVersion := "2.11.8"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.6.0"  % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.6.0"  % "provided")



