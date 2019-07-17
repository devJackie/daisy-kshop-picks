package com.kthcorp.daisy.picks

import com.kthcorp.daisy.picks.utils.{BroadcastInstance, CommonsUtil, HdfsUtil}
import org.apache.commons.logging.LogFactory
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object DevSparkRecommender {
	LogManager.getRootLogger().setLevel(Level.INFO)
	@transient lazy val log = LogFactory.getLog("DRIVER-LOG:")
	Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
	Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)

	def main(args: Array[String]): Unit = {

		try {
			// sparkAppName, profiles (LOCAL, DEVELOP, PRODUCTION) 인자 값으로 받음
			val Array(sparkAppName, profiles) = args match {
				case Array(arg0, arg1) => Array(arg0, arg1)
				case Array(arg0, _*) => Array(arg0, "PRODUCTION")
				case Array(_*) => Array("test-spark-recommender", "PRODUCTION")
			}
			// 상용
//      val sparkConf = new SparkConf().setAppName(sparkAppName)
//      sparkConf.set("spark.sql.crossJoin.enabled", "true")
//      sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

			// 테스트
			val sparkConf = new SparkConf().setMaster("local[*]").setAppName(sparkAppName)
			//            val sparkConf = new SparkConf().setMaster("spark://localhost:7077").setAppName(sparkAppName)
			sparkConf.set("spark.sql.crossJoin.enabled", "true")
			sparkConf.set("deploy-mode", "cluster")
			sparkConf.set("spark.driver.memory", "4g")
			sparkConf.set("spark.executor.memory", "8g")
//      sparkConf.set("spark.executor.cores", "2")
//      sparkConf.set("spark.executor.instances", "2")
//      sparkConf.set("spark.jars", "jars/daisy-kshop-picks_2.11-1.0.jar")
//      sparkConf.set("spark.default.parallelism", "4")
			sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

			val spark = SparkSession.builder().config(sparkConf).getOrCreate()
			// Optional, but may help avoid errors due to long lineage
			// https://stackoverflow.com/questions/31484460/spark-gives-a-stackoverflowerror-when-training-using-als
			// java StackOverflowError 시 체크 포인트 설정
			// https://issues.apache.org/jira/browse/SPARK-1006
			// ALS는 너무 긴 계보 체인 (집행자의 deserialization 원인 stackoverflow)으로 StackOverflow 발생,
			// 우리는 계보 체인을 깰 수 없기 때문에 DAGScheduler를 재귀적으로 변경하여 이 문제를 해결할수 확실하지 않음. 하지만 그것은 잠재적인 stackoverflow를 드라이버에서 피할 것이다.
			// (이 예외는 반복적으로 단계를 만들 때 발생했다)
			// 전일자 조회
			val p_yymmdd = CommonsUtil.getMinus1DaysDate()
			val p_hh = CommonsUtil.getTimeForHour()
			//			spark.sparkContext.setCheckpointDir("hdfs://daisydp/tmp/p_yymmdd=" + p_yymmdd)
			HdfsUtil.setHdfsCheckPointDir(spark, profiles, CommonsUtil.getYaml(profiles).get("COMMON").get("KSHOP-DAISY-PICKS"), p_yymmdd, p_hh)
//      spark.sparkContext.setCheckpointDir("hdfs://localhost/tmp/p_yymmdd=" + p_yymmdd)
//      spark.sparkContext.setCheckpointDir("hdfs://daisydp/tmp/p_yymmdd=" + p_yymmdd)

			// 상용
//      val base = "hdfs://daisydp/ml/test/devjackie/kth/"
			// 테스트
//			val base = "hdfs://localhost/user/devjackie/kth/"
			val base = CommonsUtil.getYaml(profiles).get("HDFS").get("HDFS_BASE_URL")
//      val rawUserItemData: Dataset[String] = spark.read.textFile(base + "kth_item_user_data.txt").repartition(10)
//      val rawUserItemData: Dataset[String] = spark.read.textFile(base + "kth_item_user_data2.txt").repartition(10)
			val rawItemData: Dataset[String] = spark.read.textFile(base + "kth_item_item_nm_data.txt").repartition(10)
//      val rawUserItemData: Dataset[String] = spark.read.textFile(base + "test_result_1month.txt").repartition(10)
//      val rawUserItemData: Dataset[String] = spark.read.textFile(base + "test_result_2month.txt").repartition(10)
			val rawUserItemData: Dataset[String] = spark.read.textFile(base + "test_result_3month.txt").repartition(10)
//      val rawUserItemData: Dataset[String] = spark.read.textFile(base + "test_result_4month.txt").repartition(10)
//      val rawUserItemData: Dataset[String] = spark.read.textFile(base + "test_result_5month.txt").repartition(10)
//      val rawUserItemData: Dataset[String] = spark.read.textFile(base + "test_result_6month.txt").repartition(10)
//      val rawUserItemData: Dataset[String] = spark.read.textFile(base + "test_result_1year.txt").repartition(10)
//      val rawUserItemData: Dataset[String] = spark.read.textFile(base + "test_result_3year.txt").repartition(10)

			val devSparkRecommenderExecute = new DevSparkRecommenderExecute(spark, p_yymmdd)

			////////////////////////////////////////////////////////
			// 구매이력 10개이하 user 가져오기
//      val rawUserIdData: Dataset[String] = spark.read.textFile("hdfs://localhost/user/devjackie/picks/result/userId3month/10under/p_yymmdd=20181104/part-00000-32576e4c-bf64-4e50-b0b4-36edf1ad1a17-c000.csv").repartition(10)
//    // 구매이력 2개이상 user 가져오기
//      val rawUserIdData: Dataset[String] = spark.read.textFile("hdfs://localhost/user/devjackie/picks/result/userId3month/2over/p_yymmdd=20181106/part-00000-cbae0088-5311-412e-aeb0-29e37b7fdcc1-c000.csv").repartition(10)
//      // 구매이력 3개이상 user 가져오기
//      val rawUserIdData: Dataset[String] = spark.read.textFile("hdfs://localhost/user/devjackie/picks/result/userId3month/3over/p_yymmdd=20181023/part-00000-ea1f8ea7-9ac6-43c7-ab15-5012b41025f9-c000.csv").repartition(10)
//      // 구매이력 4개이상 user 가져오기
//      val rawUserIdData: Dataset[String] = spark.read.textFile("hdfs://localhost/user/devjackie/picks/result/userId3month/4over/p_yymmdd=20181023/part-00000-549c7a4f-a59f-4a28-9ca8-8785d85a1eb7-c000.csv").repartition(10)
			// 구매이력 5개이상 user 가져오기
//	    val rawUserIdData: Dataset[String] = spark.read.textFile("hdfs://localhost/user/devjackie/picks/result/userId3month/5over/p_yymmdd=20181106/part-00000-98c10393-a886-4619-badb-195763f4a001-c000.csv").repartition(10)
//	    // 구매이력 6개이상 user 가져오기
//	    val rawUserIdData: Dataset[String] = spark.read.textFile("hdfs://localhost/user/devjackie/picks/result/userId3month/6over/p_yymmdd=20181023/part-00000-40442e94-3b02-4f79-87bb-d9099372fed0-c000.csv").repartition(10)
//
//	    // 구매이력 n 개이상 user 만 추출해서 추천 실행
//	    var bcUserItemData = BroadcastInstance.getBroadCastUserItemData(spark.sparkContext, spark, devSparkRecommenderExecute.preparationMakeData(rawUserItemData))
//	    var bcUserItemData = BroadcastInstance.getBroadCastUserItemData(spark.sparkContext, spark, devSparkRecommenderExecute.preparationJoinData(rawUserItemData, rawUserIdData))
//	    devSparkRecommenderExecute.recommend(rawUserItemData, rawItemData, bcUserItemData.value)
			////////////////////////////////////////////////////////

			// user, hashUser Map 데이터 생성
//      var bcUserItemData = BroadcastInstance.getBroadCastUserItemData(spark.sparkContext, spark, devSparkRecommenderExecute.preparationData(rawUserItemData, rawItemData))
			var bcUserItemData = BroadcastInstance.getBroadCastUserItemData(spark.sparkContext, spark, devSparkRecommenderExecute.preparation(rawUserItemData))
//      devSparkRecommenderExecute.model(rawUserItemData, rawItemData, bcUserItemData.value)
//      devSparkRecommenderExecute.evaluate(bcUserItemData.value)
			devSparkRecommenderExecute.evaluateTest(bcUserItemData.value)
//      devSparkRecommenderExecute.modelSave(rawUserItemData, rawItemData, bcUserItemData.value)
//      devSparkRecommenderExecute.recommend(rawUserItemData, rawItemData, bcUserItemData.value)

			// spark context hdfs checkPoint 삭제
			// http://techidiocy.com/java-lang-illegalargumentexception-wrong-fs-expected-file/
			HdfsUtil.delHdfsCheckPointDir(spark, profiles, CommonsUtil.getYaml(profiles).get("COMMON").get("KSHOP-DAISY-PICKS"), p_yymmdd, p_hh)
//			HdfsUtil.delHdfsCheckPointDir(spark, p_yymmdd)
			spark.stop()
		} catch {
			case e: Exception => log.error("", e)
		} finally {

		}
	}
}
