package com.kthcorp.daisy.picks

import com.kthcorp.daisy.picks.utils.BroadcastInstance
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable

object HashUserToUserMappingTest {
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
    
        val data = Array(
            Row(-2054210684, Array(Row(394924, 0.06045123))),
            Row(-2054210684, Array(Row(421086, 0.038403966))),
            Row(-1473375006, Array(Row(444444, 0.099999999)))
        )
    
        val schema = StructType(
            Array(
                StructField("user", IntegerType, true),
                StructField("recommendations", ArrayType(StructType(Array(
                    StructField("item", IntegerType, nullable = true),
                    StructField("rating", DoubleType, nullable = true)
                )), containsNull = true), nullable = true)
            )
        )
    
        val df = spark.createDataFrame(
            spark.sparkContext.parallelize(data),
            schema
        )
        
        import spark.implicits._
        // userItemData 조회
        val rawUserDataDF = df.mapPartitions( rdd => {
            rdd.map(x => {
                val user = (x.getAs[Int]("user"))
                val recommendations = x.getAs[mutable.WrappedArray[GenericRowWithSchema]]("recommendations")
            
                var item = 0
                var prediction = 0.0
                recommendations.map { array =>
                    item = array.getAs[Int]("item")
                    prediction = array.getAs[Double]("rating")
                    (item, prediction)
                }
                (user, item, prediction)
            })
        }).toDF("user", "item", "prediction")
        rawUserDataDF.show()
    
        // user, hashUser Map 데이터 생성
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
        
        // broadcast 등록
        var broadcastHashUser: Broadcast[scala.collection.Map[Int,String]] = BroadcastInstance.getBroadCastHashUserData(spark.sparkContext, spark, rawHashUserDataMap)
        broadcastHashUser.value.foreach(x => println(s"# x: >> $x"))
        log.info(s"# broadcastHashUser init :: ${broadcastHashUser.value.size}")
    
    
        // hashUser 와 user 매핑해서 hash 로 만들기전 user id 를 구해 dataframe 으로 변환
        val mergeDF = rawUserDataDF.mapPartitions( rdd => {
            rdd.map(x => {
                val user = (x.getAs[Int]("user"))
                val item = (x.getAs[Int]("item"))
                val prediction = (x.getAs[Double]("prediction"))
//                println(s">>>>>>>>>>>>user: $user")
//                println(s">>>>>>>>>>>>item: $item")
//                println(s">>>>>>>>>>>>prediction: $prediction")
    
                println(broadcastHashUser.value.get(user))
                val findOriUser = broadcastHashUser.value.getOrElse(user, user.toString)
                
                println(s"# findOriUser: $findOriUser")
                
//                var resSet = ("",0,0.0)
//                    if (findOriUser.size < 12) {
//
//                    } else {
//                        resSet = (findOriUser, item, prediction)
//                    }
//
//                resSet
                (findOriUser, item, prediction)
            })
        }).toDF("user", "item", "prediction")
        mergeDF.show()
    }
}
