package com.kthcorp.daisy.picks

import com.kthcorp.daisy.picks.utils.{BroadcastInstance, CommonsUtil, HdfsUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

import scala.beans.BeanProperty
import scala.collection.JavaConversions

/**
	* create by devjackie on 2018.10.17
	*/
object SparkRecommender {
	@transient lazy val log = Logger.getRootLogger()
	Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
	Logger.getLogger("org.apache.hadoop").setLevel(Level.INFO)

	def main(args: Array[String]): Unit = {

		try {
			// sparkAppName, profiles (LOCAL, DEVELOP, PRODUCTION) 인자 값으로 받음
			val Array(sparkAppName, profiles) = args match {
				case Array(arg0, arg1) => Array(arg0, arg1)
				case Array(arg0, _*) => Array(arg0, "PRODUCTION")
				case Array(_*) => Array("spark-recommender", "PRODUCTION")
			}
			log.info(s"sparkAppName : ${sparkAppName}")
			log.info(s"profiles : ${profiles}")

//			val is = getClass.getResourceAsStream("/common-dev.yaml")
//			val yml = new Yaml()
//			val yamlParsers = yml.load(is).asInstanceOf[java.util.Map[String, java.util.Map[String, String]]]
//			val hdfs = yamlParsers.get("HDFS")

//			var stream = getClass.getResourceAsStream("/common-dev-test.yaml")
//			val yaml = new Yaml(new Constructor(classOf[CustYaml]))
//			val config = yaml.load(stream).asInstanceOf[CustYaml]
//			val config = CommonsUtil.getYaml(profiles)

			val sparkConf = new SparkConf().setAppName(sparkAppName)
//			val sparkConf = new SparkConf().setMaster("local[4]").setAppName(sparkAppName)
//			sparkConf.set("spark.driver.memory", "4g")
//			sparkConf.set("spark.executor.memory", "8g")
			sparkConf.set("spark.sql.crossJoin.enabled", "true")
			sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			sparkConf.set("spark.driver.allowMultipleContexts", "true") // 여러 context 허용
			sparkConf.set("spark.files.overwrite", "true") // spark를 통해 배포 된 파일을 덮어 쓸지 여부를 제어 할 수 있음
			sparkConf.set("spark.ui.killEnabled", "true") // spark UI -> stages -> Active Stages -> +details 옆에 kill 링크 추가됨
			val spark = SparkSession.builder().config(sparkConf).getOrCreate()
			// Optional, but may help avoid errors due to long lineage
			// https://stackoverflow.com/questions/31484460/spark-gives-a-stackoverflowerror-when-training-using-als
			// java StackOverflowError 시 체크 포인트 설정
			// ALS는 너무 긴 lineage chain (집행자의 deserialization 원인 stackoverflow)으로 StackOverflow 발생,
			// 하지만 우리는 계보 체인을 깰 수 없기 때문에 DAGScheduler를 재귀적으로 변경 하여 이 문제를 해결할 경우 확실하지 않다.
			// 하지만 그것은 잠재적인 stackoverflow 드라이버에서 피할 것이다. (예외가 반복적으로 단계를 만들 때 발생했다)
			// 전일자 조회
			val p_yymmdd = CommonsUtil.getMinus1DaysDate()
			val p_hh = CommonsUtil.getTimeForHour()

			HdfsUtil.setHdfsCheckPointDir(spark, profiles, CommonsUtil.getYaml(profiles).get("COMMON").get("COMM_KSHOP_DAISY_PICKS"), p_yymmdd)

			val rawUserItemData: Dataset[String] = spark.read.textFile(CommonsUtil.getYaml(profiles).get("HDFS").get("HDFS_BASE_RESULT_3MONTH_URL")).repartition(10)
//			val rawUserItemData: Dataset[String] = spark.read.textFile(CommonsUtil.getYaml(profiles).get("HDFS").get("MART_RMD_CST_PRD_PRCH_IN_S") + "/p_yymmdd=" + p_yymmdd).repartition(10)

			val sparkRecommenderExecute = new SparkRecommenderExecute(spark, profiles, p_yymmdd, p_hh)
			// user, hashUser Map 데이터 생성
			var bcUserItemData = BroadcastInstance.getBroadCastUserItemData(spark.sparkContext, spark, sparkRecommenderExecute.preparation(rawUserItemData))
			sparkRecommenderExecute.recommend(rawUserItemData, bcUserItemData.value)

			// spark context hdfs checkPoint 삭제
			// http://techidiocy.com/java-lang-illegalargumentexception-wrong-fs-expected-file/
			HdfsUtil.delHdfsCheckPointDir(spark, profiles, CommonsUtil.getYaml(profiles).get("COMMON").get("COMM_KSHOP_DAISY_PICKS"), p_yymmdd)
			spark.stop()
		} catch {
			case e: Exception => log.error("", e)
		} finally {

		}
	}
}