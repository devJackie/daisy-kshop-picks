package com.kthcorp.daisy.picks

import java.net.URI

import com.kthcorp.daisy.picks.utils.{BroadcastInstance, CommonsUtil, HdfsUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

/**
	* create by devjackie on 2018.10.17
	*/
object SparkRecommender_V1 {
	@transient lazy val log = Logger.getRootLogger()
	Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
	Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)

	def main(args: Array[String]): Unit = {

		try {
			// sparkAppName, profiles (LOCAL, DEVELOP, PRODUCTION) 인자 값으로 받음
			val Array(sparkAppName, profiles) = args match {
				case Array(arg0, arg1) => Array(arg0, arg1)
				case Array(arg0, _*) => Array(arg0, "PRODUCTION")
				case Array(_*) => Array("dev-spark-recommender", "PRODUCTION")
			}

			val sparkConf = new SparkConf().setAppName(sparkAppName)
			sparkConf.set("spark.sql.crossJoin.enabled", "true")
			sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			val spark = SparkSession.builder().config(sparkConf).getOrCreate()
			// Optional, but may help avoid errors due to long lineage
			// https://stackoverflow.com/questions/31484460/spark-gives-a-stackoverflowerror-when-training-using-als
			// java StackOverflowError 시 체크 포인트 설정
			// ALS는 너무 긴 lineage chain (집행자의 deserialization 원인 stackoverflow)으로 StackOverflow 발생,
			// 하지만 우리는 계보 체인을 깰 수 없기 때문에 DAGScheduler를 재귀적으로 변경 하여 이 문제를 해결할 경우 확실하지 않습니다.
			// 하지만 그것은 잠재적 인 stackoverflow 드라이버에서 피할 것이다. (나는이 예외가 반복적으로 단계를 만들 때 발생했습니다)
			// 전일자 조회
			val p_yymmdd = CommonsUtil.getMinus1DaysDate()
			val p_hh = CommonsUtil.getTimeForHour()
//			spark.sparkContext.setCheckpointDir("hdfs://daisydp/tmp/p_yymmdd=" + p_yymmdd)
			HdfsUtil.setHdfsCheckPointDir(spark, profiles, CommonsUtil.getYaml(profiles).get("COMMON").get("KSHOP-DAISY-PICKS"), p_yymmdd, p_hh)


			val base = "hdfs://daisydp/ml/test/devjackie/kth/"
			val rawItemData: Dataset[String] = spark.read.textFile(base + "kth_item_item_nm_data.txt").repartition(10)
			val rawUserItemData: Dataset[String] = spark.read.textFile(base + "test_result_3month.txt").repartition(10)
			//            val rawUserItemData: Dataset[String] = spark.read.textFile(base + "test_result_6month.txt").repartition(10)
			//            val rawUserItemData: Dataset[String] = spark.read.textFile(base + "test_result_1year.txt").repartition(10)

			val sparkRecommenderExecute = new SparkRecommenderExecute_V1(spark, p_yymmdd)
			// user, hashUser Map 데이터 생성
			var bcUserItemData = BroadcastInstance.getBroadCastUserItemData(spark.sparkContext, spark, sparkRecommenderExecute.preparation(rawUserItemData))
			//        testSparkRecommenderExecute.model(rawUserItemData, rawItemData, bcUserItemData.value)
			//        testSparkRecommenderExecute.evaluate(rawUserItemData, rawItemData, bcUserItemData.value)
			sparkRecommenderExecute.recommend(rawUserItemData, rawItemData, bcUserItemData.value)

			// spark context hdfs checkPoint 삭제
			// http://techidiocy.com/java-lang-illegalargumentexception-wrong-fs-expected-file/
			HdfsUtil.delHdfsCheckPointDir(spark, profiles, CommonsUtil.getYaml(profiles).get("COMMON").get("KSHOP-DAISY-PICKS"), p_yymmdd, p_hh)
			spark.stop()
		} catch {
			case e: Exception => log.error("", e)
		} finally {

		}
	}
}
