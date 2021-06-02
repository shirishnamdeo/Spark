name := "SparkProject01_SBT"

version := "0.1"

scalaVersion := "2.12.8"


//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.0" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.0" % "provided"
// There is some issues when using the keywork "provided". Observed that the spark application is not able to run
// Getting Error:-  Exception in thread "main" java.lang.NoClassDefFoundError: org/apache/spark/SparkConf


val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

// spark-hive is needs for hive enable support in spark-session
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion


