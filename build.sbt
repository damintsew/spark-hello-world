name := "BigDataUniversity"
 
version := "1.2.0"

val sparkVersion = "1.6.0"
 
scalaVersion := "2.10.6"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.6.0"  % "provided",
  "org.apache.spark" %% "spark-hive" % "1.6.0"  % "provided",
  "commons-net" % "commons-net" % "3.5")



