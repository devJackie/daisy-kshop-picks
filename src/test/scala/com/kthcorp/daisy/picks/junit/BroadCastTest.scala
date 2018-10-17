package com.kthcorp.daisy.picks.junit

import com.kthcorp.daisy.picks.utils.BroadcastInstance
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object BroadCastTest {
    @transient lazy val log = Logger.getRootLogger()
    def main(args: Array[String]): Unit = {
    
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
        sparkConf.set("spark.sql.crossJoin.enabled", "true")
        sparkConf.set("spark.driver.memory", "4g")
        sparkConf.set("spark.executor.memory", "8g")
//        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        val base = "hdfs://localhost/user/devjackie/kth/"
        
        val dataset: Dataset[String] = spark.read.textFile(base + "userId.txt")
        
        import spark.implicits._
        val rawHashUserDataMap = dataset.flatMap { line =>
            val Array(user) = line.split(",")
            if (user.isEmpty) {
                None
            } else {
                Some((user.hashCode, user.toString))
            }
        }.collect().toMap
        rawHashUserDataMap.foreach(x =>
            println(x)
        )
        var broadcastHashUser = BroadcastInstance.getBroadCastHashUserData(spark.sparkContext, spark, rawHashUserDataMap)
        broadcastHashUser.value.foreach(x => println(s"# x: >> $x"))
        log.info(s"# broadcastHashUser init :: ${broadcastHashUser.value.size}")
    }
}
