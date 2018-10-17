package com.kthcorp.daisy.picks.junit

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

case class SimpleTuple(desc: String)
object DataSetTest {
    
    val dataList = List(
        SimpleTuple("abc"),
        SimpleTuple("bcd")
    )
    
    @transient lazy val log = Logger.getRootLogger()
    def main(args: Array[String]): Unit = {
    
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
        sparkConf.set("spark.sql.crossJoin.enabled", "true")
        sparkConf.set("spark.driver.memory", "4g")
        sparkConf.set("spark.executor.memory", "8g")
        
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    
        val base = "hdfs://localhost/user/devjackie/kth/"
        
        //201403296261, -1473375006
        //201612352737, 86810639
        val ds = Seq(
            ("201403296261"),
            ("201612352737")
//            ("")
//            ("201403296261,1111,1"),
//            ("201612352737,2222,1")
        )
        
        import spark.implicits._
        val dataset = spark.createDataset(
            spark.sparkContext.parallelize(ds)
        )
        dataset.show()
//        val dfWithoutSchema = spark.sparkContext.parallelize(ds).toDS()
//        dfWithoutSchema.show()
    
//        import spark.implicits._
//        val datasetDS = dataset.map( line => {
//            val Array(user) = line.split(",")
//            var transData = ("", 0)
//
//            if(user.isEmpty) {
////                transData = ("", 0)
//                None
//            } else {
//                transData = (user.toString, user.hashCode)
//            }
//            transData
//            }).toDF("oriUser", "user")
//        datasetDS.show()
        
    
        import spark.implicits._
        val rawHashUserItemDataDS = dataset.flatMap { line =>
            val Array(user) = line.split(",")
            if (user.isEmpty) {
                None
            } else {
                Some((user.toString, user.hashCode))
            }
        }.collect().toMap
        rawHashUserItemDataDS.foreach{ x =>
//            println(s"x.1 >> $x._1")
//            println(s"x.2 >> $x._2")
            println(x._1) // 맵의 key
        }
        rawHashUserItemDataDS.foreach{ x =>
            println(x._2) // 맵의 value
        }
    }
}
