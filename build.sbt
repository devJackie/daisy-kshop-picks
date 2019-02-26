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
//https://kun-liu.com/blog/numerical%20computing/2017/12/21/use-native-blas-and-lapack-in-apache-spark.html
//LAPACK:61 - Failed to load implementation from: com.github.fommil.netlib.NativeRefBLAS
//LAPACK:61 - Failed to load implementation from: com.github.fommil.netlib.NativeSystemLAPACK
libraryDependencies += "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()
libraryDependencies += "org.json4s" %% "json4s-native" % "3.2.10"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.10"

//scalacOptions ++= Seq(
//    "-encoding", "UTF-8",
//    "-Ymacro-debug-lite"
//)

retrieveManaged := true