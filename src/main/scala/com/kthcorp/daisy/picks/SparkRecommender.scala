package com.kthcorp.daisy.picks

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object SparkRecommender {
    @transient lazy val log = Logger.getRootLogger()
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
    
    def main(args: Array[String]): Unit = {
    
        // 필수 파라미터 체크 (app 명)
        val Array(sparkAppName) = args
        if(args.length != 1) {
            System.exit(1)
        }
        
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName(sparkAppName)
        sparkConf.set("spark.sql.crossJoin.enabled", "true")
        sparkConf.set("spark.driver.memory", "4g")
        sparkConf.set("spark.executor.memory", "8g")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        // Optional, but may help avoid errors due to long lineage
//        spark.sparkContext.setCheckpointDir("hdfs://localhost/tmp/")
//        spark.sparkContext.setCheckpointDir("hdfs://daisydp/tmp/")

        val base = "hdfs://localhost/user/devjackie/kth/"
//        val base = "hdfs://daisydp/ml/test/devjackie/kth/"
//        val rawUserItemData: Dataset[String] = spark.read.textFile(base + "kth_item_user_data.txt").repartition(10)
//        val rawUserItemData: Dataset[String] = spark.read.textFile(base + "kth_item_user_data2.txt").repartition(10)
        val rawItemData: Dataset[String] = spark.read.textFile(base + "kth_item_item_nm_data.txt").repartition(10)
//        val rawUserItemData: Dataset[String] = spark.read.textFile(base + "test_result_3month.txt").repartition(10)
        val rawUserItemData: Dataset[String] = spark.read.textFile(base + "test_result_6month.txt").repartition(10)
//        val rawUserItemData: Dataset[String] = spark.read.textFile(base + "test_result_1year.txt").repartition(10)
        
        val sparkRecommenderExecute = new SparkRecommenderExecute(spark)
        val newRawUserItemData = sparkRecommenderExecute.preparation(rawUserItemData)
//        sparkRecommenderExecute.model(rawUserItemData, rawItemData, newRawUserItemData)
        sparkRecommenderExecute.evaluate(rawUserItemData, rawItemData, newRawUserItemData)
//        sparkRecommenderExecute.recommend(rawUserItemData, rawItemData, newRawUserItemData)

    }
}
