package com.kthcorp.daisy.picks

import com.kthcorp.daisy.picks.utils.BroadcastInstance
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.sources.IsNotNull
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import util.control.Breaks._
import scala.collection.mutable

object FilteringExistRecommListTest {
    @transient lazy val log = Logger.getRootLogger()
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
    
    case class TestPerson(name: String, age: Long, salary: Double)
    
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
    

        // 1-1 user 구매내역정보와 user 추천결과정보를 매핑
        val join1 = recommDf.select('user as "user", 'item as "item", 'prediction as "prediction")
            .join(oriDf.select('user as "oriUser", 'item as "oriItem", 'count as "oriCount"), ($"user" === $"oriUser") && ($"item" === $"oriItem"))
        join1.show()
        // 1-2 user 추천결과정보에서 1-1 의 매핑 정보를 제외
        recommDf.select('user as "user", 'item as "item", 'prediction as "prediction")
            .except(join1.select('user as "user", 'item as "item", 'prediction as "prediction")).show()
        
        
        
        val array = Array(145044192, 407160, 1)
        val m = new mutable.HashMap[String, Int]()
        m.put("user", 145044192)
        m.put("item", 407160)
        m.put("count", 1)
        val listBf = mutable.ListBuffer[mutable.HashMap[String, Int]]()
        listBf.append(m)
        listBf.foreach{map =>
            val oriUser = map.get("user")
            val oriItem = map.get("item")
            val oriCount = map.get("count")
            
        }
        val listBf2 = mutable.ListBuffer[Tuple3[Int,Int,Double]]()
        val m2 = new mutable.HashMap[String, Int]()
        var check = false
        val testDf = recommDf.mapPartitions( rdd => {
            rdd.map( x => {
//                val user = (x.getAs[Int]("user"))
//                val item = (x.getAs[Int]("item"))
//                val prediction = (x.getAs[Double]("prediction"))
//                var filterUser = 0
//                var filterItem = 0
//                var filterCount = 0
//
                
                val aaa = listBf.foreach { map =>
                    val user = (x.getAs[Int]("user"))
                    val item = (x.getAs[Int]("item"))
                    val prediction = (x.getAs[Double]("prediction"))
                    breakable {
                        val oriUser = map.getOrElse("user", 0)
                        val oriItem = map.getOrElse("item", 0)
                        if (user == map.getOrElse("user", 0) && item == map.getOrElse("item", 0)) {
                        
                        } else {
                            check = true
//                            m2.put("user", user.toInt)
//                            m2.put("item", item.toInt)
//                            m2.put("prediction", prediction.toInt)
                            
                            listBf2.append((user, item, prediction))
                            break
                        }
                    }
//                    Tuple3(user, item, prediction)
//                    Some(user, item, prediction)
                }
//                if (check == true)

//                    (aaa.apply(0)._1, aaa.apply(0)._2, aaa.apply(0)._3)
//                else
//                    Tuple3(0, 0, 0)
//                (listBf2.apply(0)._1, listBf2.apply(0)._2, listBf2.apply(0)._3)
                listBf2.map{ bf =>
                    (bf._1, bf._2, bf._3)
                }
            })


//        }).toDF("user", "item", "prediction")
        })
        log.info(s">>>>>>> testDf show!!")
        testDf.show(false)
        case class UserInfo(user: Int, item: Int, prediction: Double)
    
        listBf2.map{ bf =>
            println(s">> $bf._1, bf._2, bf._3")
        }
        listBf2.toDF("user", "item", "prediction").show()

        
        
        val tom = TestPerson("Tom Hanks",37,35.5)
        val sam = TestPerson("Sam Smith",40,40.5)
        val PersonList = mutable.MutableList[TestPerson]()
        PersonList += tom
        PersonList += sam
        
        val personDF = PersonList.toDF()
        println(personDF.getClass)
        personDF.show()
        personDF.select("name", "age").show()
    
    
        //        val testDf = recommDf.mapPartitions( rdd => {
//            rdd.map( x => {
//                val user = (x.getAs[Int]("user"))
//                val item = (x.getAs[Int]("item"))
//                val prediction = (x.getAs[Double]("prediction"))
//                var filterUser = 0
//                var filterItem = 0
//                var filterCount = 0
//
//                oriDf.foreachPartition(rdd2 => {
//                    rdd2.foreach(xx => {
//                        val oriUser = (xx.getAs[Int]("user"))
//                        val oriItem = (xx.getAs[Int]("item"))
//                        val oriCount = (xx.getAs[Int]("count"))
//                        if (user == oriUser){
//                            filterUser = oriUser
//                            filterItem = oriItem
//                            filterCount = oriCount
//                        }
//                    })
//                })
//                (filterUser, filterItem, filterCount)
//            })
//
//
//        }).toDF("user", "item", "count")
//        log.info(s">>>>>>> testDf show!!")
//        testDf.show(false)
        
//        val rawUserDataDF = df.mapPartitions( rdd => {
//            rdd.map(x => {
//                val user = (x.getAs[Int]("user"))
//                val recommendations = x.getAs[mutable.WrappedArray[GenericRowWithSchema]]("recommendations")
//
//                var item = 0
//                var prediction = 0.0
//                recommendations.map { array =>
//                    item = array.getAs[Int]("item")
//                    prediction = array.getAs[Double]("rating")
//                    (item, prediction)
//                }
//                (user, item, prediction)
//            })
//        }).toDF("user", "item", "prediction")
//        rawUserDataDF.show()
//
//        // user, hashUser Map 데이터 생성
//        val rawHashUserDataMap = dataSet.flatMap { line =>
//            val Array(user) = line.split(",")
//            if (user.isEmpty) {
//                None
//            } else {
//                Some((user.hashCode, user.toString))
//            }
//        }.collect().toMap
//        rawHashUserDataMap.foreach(x =>
//            println(x)
//        )
//
//        // broadcast 등록
//        var broadcastHashUser: Broadcast[Map[Int, String]] = BroadcastInstance.getBroadCastHashUserData(spark.sparkContext, spark, rawHashUserDataMap)
//        broadcastHashUser.value.foreach(x => println(s"# x: >> $x"))
//        log.info(s"# broadcastHashUser init :: ${broadcastHashUser.value.size}")
//
//
//        // hashUser 와 user 매핑해서 hash 로 만들기전 user id 를 구해 dataframe 으로 변환
//        val mergeDF = rawUserDataDF.mapPartitions( rdd => {
//            rdd.map(x => {
//                val user = (x.getAs[Int]("user"))
//                val item = (x.getAs[Int]("item"))
//                val prediction = (x.getAs[Double]("prediction"))
////                println(s">>>>>>>>>>>>user: $user")
////                println(s">>>>>>>>>>>>item: $item")
////                println(s">>>>>>>>>>>>prediction: $prediction")
//
//                println(broadcastHashUser.value.get(user))
//                val findOriUser = broadcastHashUser.value.getOrElse(user, user.toString)
//
//                println(s"# findOriUser: $findOriUser")
//
////                var resSet = ("",0,0.0)
////                    if (findOriUser.size < 12) {
////
////                    } else {
////                        resSet = (findOriUser, item, prediction)
////                    }
////
////                resSet
//                (findOriUser, item, prediction)
//            })
//        }).toDF("user", "item", "prediction")
//        mergeDF.show()
    }
}
