package com.kthcorp.daisy.picks

import com.kthcorp.daisy.picks.utils.{BroadcastInstance, CommonsUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object TestSparkRecommender {
	@transient lazy val log = Logger.getRootLogger()
	Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
	Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)

	def main(args: Array[String]): Unit = {

		try {
			// 필수 파라미터 체크 (app 명)
			val Array(sparkAppName) = args
			if (args.length != 1) {
				System.exit(1)
			}

			val sparkConf = new SparkConf().setMaster("local[*]").setAppName(sparkAppName)
			//            val sparkConf = new SparkConf().setAppName(sparkAppName)
			sparkConf.set("spark.sql.crossJoin.enabled", "true")
			sparkConf.set("spark.driver.memory", "4g")
			sparkConf.set("spark.executor.memory", "8g")
			sparkConf.set("spark.executor.cores", "2")
			sparkConf.set("spark.default.parallelism", "4")
			sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			val spark = SparkSession.builder().config(sparkConf).getOrCreate()
			// Optional, but may help avoid errors due to long lineage
			// https://stackoverflow.com/questions/31484460/spark-gives-a-stackoverflowerror-when-training-using-als
			// java StackOverflowError 시 체크 포인트 설정
			// ALS는 너무 긴 계보 체인 (집행자의 deserialization 원인 stackoverflow)으로 StackOverflow 발생,
			// 하지만 우리는 계보 체인을 깰 수 없기 때문에 DAGScheduler를 재귀적으로 변경 하여 이 문제를 해결할 경우 확실하지 않습니다.
			// 하지만 그것은 잠재적 인 stackoverflow 드라이버에서 피할 것이다. (나는이 예외가 반복적으로 단계를 만들 때 발생했습니다)
			// 전일자 조회
			val p_yymmdd = CommonsUtil.getMinus1DaysDate()
			spark.sparkContext.setCheckpointDir("hdfs://localhost/tmp/p_yymmdd=" + p_yymmdd)
			//        spark.sparkContext.setCheckpointDir("hdfs://daisydp/tmp/p_yymmdd=" + p_yymmdd)

			val base = "hdfs://localhost/user/devjackie/kth/"
			//            val base = "hdfs://daisydp/ml/test/devjackie/kth/"
			//            val rawUserItemData: Dataset[String] = spark.read.textFile(base + "kth_item_user_data.txt").repartition(10)
			//            val rawUserItemData: Dataset[String] = spark.read.textFile(base + "kth_item_user_data2.txt").repartition(10)
			val rawItemData: Dataset[String] = spark.read.textFile(base + "kth_item_item_nm_data.txt").repartition(10)
			//            val rawUserItemData: Dataset[String] = spark.read.textFile(base + "test_result_1month.txt").repartition(10)
			//            val rawUserItemData: Dataset[String] = spark.read.textFile(base + "test_result_3month.txt").repartition(10)
			val rawUserItemData: Dataset[String] = spark.read.textFile(base + "test_result_6month.txt").repartition(10)
			//            val rawUserItemData: Dataset[String] = spark.read.textFile(base + "test_result_1year.txt").repartition(10)

			val testSparkRecommenderExecute = new TestSparkRecommenderExecute(spark)
			// user, hashUser Map 데이터 생성
			var bcUserItemData = BroadcastInstance.getBroadCastUserItemData(spark.sparkContext, spark, testSparkRecommenderExecute.preparation(rawUserItemData))
			//            val newRawUserItemDataDF = testingSparkRecommenderExecute.preparation(rawUserItemData)
			//            testSparkRecommenderExecute.model(rawUserItemData, rawItemData, bcUserItemData.value)
			//            testSparkRecommenderExecute.evaluate(rawUserItemData, rawItemData, bcUserItemData.value)
			testSparkRecommenderExecute.recommend(rawUserItemData, rawItemData, bcUserItemData.value)
			//            testingSparkRecommenderExecute.recommend(rawUserItemData, rawItemData, newRawUserItemDataDF)

			spark.stop()
		} catch {
			case e: Exception => log.error("", e)
		} finally {

		}
	}
}
