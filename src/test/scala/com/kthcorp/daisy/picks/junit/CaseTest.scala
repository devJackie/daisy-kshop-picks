package com.kthcorp.daisy.picks.junit

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object CaseTest {
    @transient lazy val log = Logger.getRootLogger()
    def main(args: Array[String]): Unit = {
    
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
        sparkConf.set("spark.sql.crossJoin.enabled", "true")
        sparkConf.set("spark.driver.memory", "4g")
        sparkConf.set("spark.executor.memory", "8g")
//        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        // data 준비
        val oriDataSet = Seq(
            ("201604428685,407160,1")
        )
    
        val recommDataSet = Seq(
            ("201604428685,407160,1"),
            ("201604428685,365758,1"),
            ("201604428685,413175,1")
        )
        
        import spark.implicits._
        val oriDs: Dataset[String] = spark.createDataset(
            spark.sparkContext.parallelize(oriDataSet)
        )
        oriDs.show()
        
        val oriDf = oriDs.map { line =>
            val len = line.split(",").length match {
                case 2 => 2
                case 3 => 3
                case _ => 1
            }
    
            //            val Array(user, item, count) = len match {
            //                case 2 => Array(user, item) = line.split(",", 2)
            //                case 3 => Array(user, item, count) = line.split(",", 3)
            //                case _ => Array(user) = line.split(",", 1)
            //            }
            
    
            val transData = line.split(",") match {
                case Array(user, item, _*) => Array(user.hashCode, item.toInt, 0)
                case Array(user, item, count) => Array(user.hashCode, item.toInt, count.toInt)
                case Array(user, _*) => Array(user.hashCode, 0, 0)
            }
            val transData1 = Tuple3(transData(0), transData(1), transData(2))
            transData1
        }.toDF("user", "item", "count")
    
        oriDf.show(false)
    }
}
