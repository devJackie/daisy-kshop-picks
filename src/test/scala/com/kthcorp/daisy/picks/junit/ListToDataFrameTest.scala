package com.kthcorp.daisy.picks.junit

import com.kthcorp.daisy.picks.FilteringExistRecommListTest.log
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable
import scala.util.control.Breaks._

object ListToDataFrameTest {
    @transient lazy val log = Logger.getRootLogger()
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
    
    case class TestPerson(name: String, age: Long, salary: Double)
    case class RecommUserInfo(user: Int, item: Int, prediction: Double)
    case class OriUserInfo(user: Int, item: Int, prediction: Double)
    
    def main(args: Array[String]): Unit = {
    
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
        sparkConf.set("spark.sql.crossJoin.enabled", "true")
        sparkConf.set("spark.driver.memory", "4g")
        sparkConf.set("spark.executor.memory", "8g")
//        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        // data 준비
        val oriDataSet = Seq(
            ("145044192,407160,1"), //ori user 201604428685
            ("145044191,407160,1") //ori user 201604428685
        )
        
        val data = Array(
            Row(145044192, Array(Row(407160, 0.06045123))),
            Row(145044192, Array(Row(365758, 0.038403966))),
            Row(145044192, Array(Row(413175, 0.18487754))),
            Row(145044191, Array(Row(407160, 0.22222222))),
            Row(145044191, Array(Row(407161, 0.33333333)))
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
        
        val recommDfSchema = spark.createDataFrame(
            spark.sparkContext.parallelize(data),
            schema
        )
        
        import spark.implicits._
        val oriDs: Dataset[String] = spark.createDataset(
            spark.sparkContext.parallelize(oriDataSet)
        )
//        oriDs.show()
        
        val oriDf = oriDs.map { line =>
            // 원천 data 예외처리
            val transData = line.split(",") match {
                case Array(user, item, count) => Array(user.toInt, item.toInt, count.toInt)
                case Array(user, item, _*) => Array(user.toInt, item.toInt, 0)
                case Array(user, _*) => Array(user.toInt, 0, 0)
            }
            val tupleRes = Tuple3(transData(0), transData(1), transData(2))
            tupleRes
        }.toDF("user", "item", "count")
    
        oriDf.show(false)
    
        val recommDf = recommDfSchema.mapPartitions( rdd => {
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
        
        recommDf.show(false)
        
        val test = recommDf.map ( x => {
            val user = (x.getAs[Int]("user"))
            val item = (x.getAs[Int]("item"))
            val prediction = (x.getAs[Double]("prediction"))
            println(s"user : $user")
            println(s"item : $item")
            println(s"prediction : $prediction")
            
            RecommUserInfo(user, item, prediction)
        }).toDF("user", "item", "prediction")
        println(s"test show!!!")
        test.show()
        
        val test1 = recommDf.map ( x => {
            val user = (x.getAs[Int]("user"))
            val item = (x.getAs[Int]("item"))
            val prediction = (x.getAs[Double]("prediction"))
            println(s"user : $user")
            println(s"item : $item")
            println(s"prediction : $prediction")
    
//            val test2 = oriDf.map( ori => {
//                val oriUser = (ori.getAs[Int]("user"))
//                val oriItem = (ori.getAs[Int]("item"))
//                if (user == oriUser && item == oriItem) {
//
//                } else {
////                    OriUserInfo(user, item, prediction)
//                }
//                OriUserInfo(user, item, prediction)
//            })
            
            RecommUserInfo(user, item, prediction)
        }).toDF("user", "item", "prediction")
        println(s"test show!!!")
        test1.show()
        
//        val userInfoDF = userInfoBf.toDF()
//        log.info(s">>>>>>> userInfoDF show!!")
//        userInfoDF.show()
//
//        val tom = TestPerson("Tom Hanks",37,35.5)
//        val sam = TestPerson("Sam Smith",40,40.5)
//        val PersonList = mutable.MutableList[TestPerson]()
//        PersonList += tom
//        PersonList += sam
//
//        val personDF = PersonList.toDF()
//        println(personDF.getClass)
//        personDF.show()
//        personDF.select("name", "age").show()
    }
}
