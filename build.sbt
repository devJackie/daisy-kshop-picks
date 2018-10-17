name := "daisy-kshop-picks"

version := "1.0"

scalaVersion := "2.11.12"

//scalacOptions += "-Ymacro-debug-lite"

val sparkVersion = "2.3.1"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion,
    "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
    "com.google.code.gson" % "gson" % "2.8.0",
    "junit" % "junit" % "4.12",
    "org.yaml" % "snakeyaml" % "1.17"
)

libraryDependencies += "org.json4s" %% "json4s-native" % "3.2.10"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.10"

//scalacOptions ++= Seq(
//    "-encoding", "UTF-8",
//    "-Ymacro-debug-lite"
//)

retrieveManaged := true