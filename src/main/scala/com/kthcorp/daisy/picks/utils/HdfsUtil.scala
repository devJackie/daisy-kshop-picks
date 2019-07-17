package com.kthcorp.daisy.picks.utils

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
	* create by devjackie on 2018.10.17
	*/
object HdfsUtil {
	@transient lazy val log = Logger.getRootLogger()
	Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
	Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)

	def setHdfsCheckPointDir(spark: SparkSession, profiles: String, gubn: String, p_yymmdd: String): Unit = {
		spark.sparkContext.setCheckpointDir(CommonsUtil.getYaml(profiles).get("HDFS").get("HDFS_TMP_URL") + gubn + "/p_yymmdd=" + p_yymmdd)
	}

	def setHdfsCheckPointDir(spark: SparkSession, profiles: String, gubn: String, p_yymmdd: String, p_hh: String): Unit = {
		spark.sparkContext.setCheckpointDir(CommonsUtil.getYaml(profiles).get("HDFS").get("HDFS_TMP_URL") + gubn + "/p_yymmdd=" + p_yymmdd + "/p_hh=" + p_hh)
	}

	def delHdfsCheckPointDir(spark: SparkSession, profiles: String, gubn: String, p_yymmdd: String): Unit = {
		val hadoopConf = spark.sparkContext.hadoopConfiguration
		val fs = FileSystem.get(new URI(CommonsUtil.getYaml(profiles).get("HDFS").get("HDFS_DEFAULT_URL")), new Configuration(hadoopConf))
		val checkPointPath = new Path(CommonsUtil.getYaml(profiles).get("HDFS").get("HDFS_TMP_URL") + gubn + "/p_yymmdd=" + p_yymmdd )

		if (fs.exists(checkPointPath)) {
			//	    log.info(fs.getFileStatus(checkPointPath))
			fs.delete(checkPointPath, true)
		}
	}

	def delHdfsCheckPointDir(spark: SparkSession, profiles: String, gubn: String, p_yymmdd: String, p_hh: String): Unit = {
		val hadoopConf = spark.sparkContext.hadoopConfiguration
		val fs = FileSystem.get(new URI(CommonsUtil.getYaml(profiles).get("HDFS").get("HDFS_DEFAULT_URL")), new Configuration(hadoopConf))
		val checkPointPath = new Path(CommonsUtil.getYaml(profiles).get("HDFS").get("HDFS_TMP_URL") + gubn + "/p_yymmdd=" + p_yymmdd + "/p_hh=" + p_hh)

		if (fs.exists(checkPointPath)) {
//	    log.info(fs.getFileStatus(checkPointPath))
			fs.delete(checkPointPath, true)
		}
	}

	def saveAsHdfsForRecomm(spark: SparkSession, df: DataFrame, profiles: String, gubn: String, p_yymmdd: String): Unit = {
		df.coalesce(1).write
			.mode(SaveMode.Overwrite)
			.option("delimiter", "\036").csv(CommonsUtil.getYaml(profiles).get("HDFS").get("HDFS_MART_URL") + gubn + "/p_yymmdd=" + p_yymmdd)

		val hadoopConf = spark.sparkContext.hadoopConfiguration
		val fs = FileSystem.get(new URI(CommonsUtil.getYaml(profiles).get("HDFS").get("HDFS_DEFAULT_URL")), new Configuration(hadoopConf))
		val file = fs.globStatus(new Path(CommonsUtil.getYaml(profiles).get("HDFS").get("HDFS_MART_URL") + gubn + "/p_yymmdd=" + p_yymmdd + "/" + "part*"))(0).getPath().getName()
		log.info(s"file : ${file}")
		fs.rename(new Path(CommonsUtil.getYaml(profiles).get("HDFS").get("HDFS_MART_URL") + gubn + "/p_yymmdd=" + p_yymmdd  + "/" + file)
			, new Path(CommonsUtil.getYaml(profiles).get("HDFS").get("HDFS_MART_URL") + gubn + "/p_yymmdd=" + p_yymmdd  + "/" + gubn + "_" + p_yymmdd + ".csv"))
	}

	def saveAsHdfsForRecomm(spark: SparkSession, df: DataFrame, profiles: String, gubn: String, p_yymmdd: String, p_hh: String): Unit = {
		df.coalesce(1).write
			.mode(SaveMode.Overwrite)
			.option("delimiter", "\036").csv(CommonsUtil.getYaml(profiles).get("HDFS").get("HDFS_MART_URL") + gubn + "/p_yymmdd=" + p_yymmdd + "/p_hh=" + p_hh)

		val hadoopConf = spark.sparkContext.hadoopConfiguration
		val fs = FileSystem.get(new URI(CommonsUtil.getYaml(profiles).get("HDFS").get("HDFS_DEFAULT_URL")), new Configuration(hadoopConf))
		val file = fs.globStatus(new Path(CommonsUtil.getYaml(profiles).get("HDFS").get("HDFS_MART_URL") + gubn + "/p_yymmdd=" + p_yymmdd + "/p_hh=" + p_hh + "/" + "part*"))(0).getPath().getName()
		log.info(s"file : ${file}")
		fs.rename(new Path(CommonsUtil.getYaml(profiles).get("HDFS").get("HDFS_MART_URL") + gubn + "/p_yymmdd=" + p_yymmdd + "/p_hh=" + p_hh + "/" + file)
			, new Path(CommonsUtil.getYaml(profiles).get("HDFS").get("HDFS_MART_URL") + gubn + "/p_yymmdd=" + p_yymmdd + "/p_hh=" + p_hh + "/" + gubn + "_" + p_yymmdd + ".csv"))
	}

	def saveAsHdfsForSearch(df: DataFrame, profiles: String, gubn: String, p_yymmdd: String, p_hh: String): Unit = {
		df.coalesce(1).write
			.mode(SaveMode.Append)
			.option("delimiter", "\036").csv(CommonsUtil.getYaml(profiles).get("HDFS").get("HDFS_MART_URL") + gubn + "/p_yymmdd=" + p_yymmdd + "/p_hh=" + p_hh)
	}

	def setHdfsCheckPointDir(spark: SparkSession, p_yymmdd: String): Unit = {
		// 상용
//    spark.sparkContext.setCheckpointDir("hdfs://daisydp/tmp/p_yymmdd=" + p_yymmdd)
		// 테스트
		spark.sparkContext.setCheckpointDir("hdfs://localhost/tmp/p_yymmdd=" + p_yymmdd)
	}

	def delHdfsCheckPointDir(spark: SparkSession, p_yymmdd: String): Unit = {
		val hadoopConf = spark.sparkContext.hadoopConfiguration
		// 상용
//    val fs = FileSystem.get(new URI("hdfs://daisydp"), new Configuration(hadoopConf))
//    val checkPointPath = new Path("hdfs://daisydp/tmp/p_yymmdd=" + p_yymmdd + "/")
		// 테스트
		val fs = FileSystem.get(new URI("hdfs://localhost"), new Configuration(hadoopConf))
		val checkPointPath = new Path("hdfs://localhost/tmp/p_yymmdd=" + p_yymmdd + "/")
		if (fs.exists(checkPointPath)) {
			log.info(fs.getFileStatus(checkPointPath))
			fs.delete(checkPointPath, true)
		}
	}

	def devSaveAsHdfsForRecomm(finalRecommDF: DataFrame, p_yymmdd: String): Unit = {
//        finalRecommDF.withColumn("prediction", expr("CAST(prediction AS FLOAT)")).coalesce(1).write
		finalRecommDF.coalesce(1).write
			.mode(SaveMode.Overwrite)
//            .format("com.databricks.spark.csv")
//            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/1month/p_yymmdd=" + p_yymmdd)
//            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/2month/p_yymmdd=" + p_yymmdd)
//            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/3month/p_yymmdd=" + p_yymmdd)
//            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/4month/p_yymmdd=" + p_yymmdd)
//            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/5month/p_yymmdd=" + p_yymmdd)
//            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/6month/p_yymmdd=" + p_yymmdd)
//            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/1year/p_yymmdd=" + p_yymmdd)
//            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/3year/p_yymmdd=" + p_yymmdd)

//            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/3month/tuning_10_1.0_1.0/p_yymmdd=" + p_yymmdd)

			.option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/userId3month/recomm/5over/p_yymmdd=" + p_yymmdd)
//            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/userId3month/recomm/3over/p_yymmdd=" + p_yymmdd)
//            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/userId3month/recomm/4over/p_yymmdd=" + p_yymmdd)
//            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/userId3month/recomm/6over/p_yymmdd=" + p_yymmdd)
//            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/userId3month/test/6over/p_yymmdd=" + p_yymmdd)

		// 상용
//            .option("delimiter", "\036").csv("hdfs://daisydp/ml/test/devjackie/picks/result/3month/p_yymmdd=" + p_yymmdd)
	}

	def saveAsHdfsForRecomm(finalRecommDF: DataFrame, p_yymmdd: String): Unit = {
//        finalRecommDF.withColumn("prediction", expr("CAST(prediction AS FLOAT)")).coalesce(1).write
		finalRecommDF.coalesce(1).write
			.mode(SaveMode.Overwrite)
//            .format("com.databricks.spark.csv")
//            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/1month/p_yymmdd=" + p_yymmdd)
//            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/2month/p_yymmdd=" + p_yymmdd)
			.option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/3month/p_yymmdd=" + p_yymmdd)
//            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/4month/p_yymmdd=" + p_yymmdd)
//            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/5month/p_yymmdd=" + p_yymmdd)
//            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/6month/p_yymmdd=" + p_yymmdd)
//            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/1year/p_yymmdd=" + p_yymmdd)
//            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/3year/p_yymmdd=" + p_yymmdd)
	}


	//    def devSaveAsHdfsForRecomm(finalRecommDF: DataFrame, p_yymmdd: String): Unit = {
	////        finalRecommDF.withColumn("prediction", expr("CAST(prediction AS FLOAT)")).coalesce(1).write
	//        finalRecommDF.coalesce(1).write
	//            .mode(SaveMode.Overwrite)
	////            .format("com.databricks.spark.csv")
	////            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/1month/p_yymmdd=" + p_yymmdd)
	////            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/2month/p_yymmdd=" + p_yymmdd)
	////            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/3month/p_yymmdd=" + p_yymmdd)
	////            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/4month/p_yymmdd=" + p_yymmdd)
	////            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/5month/p_yymmdd=" + p_yymmdd)
	////            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/6month/p_yymmdd=" + p_yymmdd)
	////            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/1year/p_yymmdd=" + p_yymmdd)
	////            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/3year/p_yymmdd=" + p_yymmdd)
	//
	////            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/3month/tuning_10_1.0_1.0/p_yymmdd=" + p_yymmdd)
	//
	//            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/userId3month/recomm/5over/p_yymmdd=" + p_yymmdd)
	////            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/userId3month/recomm/3over/p_yymmdd=" + p_yymmdd)
	////            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/userId3month/recomm/4over/p_yymmdd=" + p_yymmdd)
	////            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/userId3month/recomm/6over/p_yymmdd=" + p_yymmdd)
	////            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/userId3month/test/6over/p_yymmdd=" + p_yymmdd)
	//
	//            // 상용
	////            .option("delimiter", "\036").csv("hdfs://daisydp/ml/test/devjackie/picks/result/3month/p_yymmdd=" + p_yymmdd)
	//    }
	//
	//    def saveAsHdfsForRecomm(finalRecommDF: DataFrame, p_yymmdd: String): Unit = {
	////        finalRecommDF.withColumn("prediction", expr("CAST(prediction AS FLOAT)")).coalesce(1).write
	//        finalRecommDF.coalesce(1).write
	//            .mode(SaveMode.Overwrite)
	////            .format("com.databricks.spark.csv")
	////            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/1month/p_yymmdd=" + p_yymmdd)
	////            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/2month/p_yymmdd=" + p_yymmdd)
	//            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/3month/p_yymmdd=" + p_yymmdd)
	////            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/4month/p_yymmdd=" + p_yymmdd)
	////            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/5month/p_yymmdd=" + p_yymmdd)
	////            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/6month/p_yymmdd=" + p_yymmdd)
	////            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/1year/p_yymmdd=" + p_yymmdd)
	////            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/3year/p_yymmdd=" + p_yymmdd)
	//    }
}
