package com.kthcorp.daisy.picks.junit

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object DataFrameSetTest2 {
    // sample
    //    |-2054210684|[[394924, 0.06045123], [421086, 0.038403966], [407161, 0.036271237], [422724, 0.03276459], [401957, 0.028334403], [391021, 0.027915189], [416055, 0.027851388], [398664, 0.024663068], [433101, 0.023624228], [432156, 0.023341784]]         |
    //    |-2052218679|[[409836, 0.18487754], [407161, 0.1697433], [402904, 0.14814107], [387788, 0.13071004], [400982, 0.10905515], [391021, 0.10789431], [435826, 0.093409784], [401957, 0.08016312], [421022, 0.058850937], [398664, 0.053960357]]               |
    //    |-2050393772|[[407628, 0.3506828], [407598, 0.28768104], [394924, 0.25809765], [420979, 0.25016508], [392695, 0.23684075], [407160, 0.21215697], [417687, 0.18077876], [407472, 0.16843115], [402906, 0.14778857], [394075, 0.14761835]]                  |
    //    |-1954011571|[[425543, 0.21217224], [406545, 0.19312713], [433101, 0.16122313], [409836, 0.13697639], [421022, 0.124423444], [414564, 0.10156901], [401957, 0.09837874], [432156, 0.098036826], [433207, 0.09561288], [431896, 0.0906331]]                |
    //    |-1953859545|[[394924, 0.0062991516], [387788, 0.006077781], [407160, 0.0060373135], [421022, 0.0060168477], [391021, 0.0058553605], [398664, 0.0053246566], [426102, 0.0052998136], [432156, 0.0048521464], [407981, 0.004506928], [407598, 0.004498824]]|
    //    |-1952847645|[[407161, 0.30412504], [401957, 0.25484085], [412045, 0.14994928], [409836, 0.12827355], [406545, 0.11879053], [421086, 0.11813481], [431896, 0.108276375], [425543, 0.09603082], [402904, 0.075544305], [411422, 0.066625655]]              |
    //    |-1952074223|[[394924, 0.05870343], [422724, 0.05578921], [421086, 0.055498958], [407161, 0.051766075], [416055, 0.0425928], [401957, 0.036967706], [404954, 0.030192137], [398664, 0.029734084], [387788, 0.027729876], [423490, 0.023632355]]           |
    //    |-1950224273|[[391021, 0.00532301], [426102, 0.005290023], [421022, 0.0051451065], [432156, 0.005092926], [398664, 0.0047162957], [407160, 0.0041515604], [387788, 0.003968256], [407981, 0.0038367435], [414564, 0.0036553561], [394924, 0.003547721]]   |
    //    |-1949151767|[[406545, 0.06139173], [412045, 0.057916157], [407161, 0.056530733], [409836, 0.04967785], [401957, 0.04870779], [407628, 0.04324519], [421086, 0.034251165], [414564, 0.033607513], [433101, 0.031923324], [426102, 0.030050375]]           |
    //    |-1946387924|[[407628, 0.07275869], [409836, 0.06806198], [421022, 0.04092877], [435826, 0.03188771], [387788, 0.027124356], [400982, 0.023400623], [407160, 0.02323428], [394924, 0.021884898], [417687, 0.021365669], [406545, 0.02132114]]
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
        sparkConf.set("spark.sql.crossJoin.enabled", "true")
        sparkConf.set("spark.driver.memory", "4g")
        sparkConf.set("spark.executor.memory", "8g")
    
        case class Recomm(user: Int, recommends: Double)
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    
        val array1 = Array(394924, 0.06045123)
        val array2 = Array(421086, 0.038403966)
        val list1 = new ListBuffer[Array[Double]]()
        list1.append(array1)
        list1.append(array2)
    
//        val schema = StructType(Array(StructField("id", IntegerType, false),
//            (StructField("emailBody", DoubleType, false))))
//
//        val my_schema = StructType(Seq(
//            StructField("field1", StringType, nullable = false),
//            StructField("field2", StringType, nullable = false)
//        ))

//        val aa = new StructType()
//        aa.add("item", IntegerType)
        
        val data = Array(
            Row(-2054210684, Array(Row(394924, 0.06045123))),
            Row(-2054210684, Array(Row(421086, 0.038403966))),
            Row(-2052218679, Array(Row(409836, 0.18487754))),
            Row(-2050393772, Array(Row(407628, 0.3506828)))
        )
//        Array(Row(ArrayBuffer(1,2,3,4)))
    
        
    
        val schema = StructType(
            Array(
                StructField("user", IntegerType, true),
//                StructField("recommends", DoubleType, true)
//                StructField("recommendations", ArrayType(StructType(StructField("item",IntegerType,true), StructField("rating",FloatType,true)),true),true),
//                StructField("recommendations", ArrayType(StructType(Array(StructField("item", IntegerType, nullable = true), StructField("rating", DoubleType, nullable = true), true)),true)
                StructField("recommendations", ArrayType(StructType(Array(
                    StructField("item", IntegerType, nullable = true),
                    StructField("rating", DoubleType, nullable = true)
                )), containsNull = true), nullable = true)
            )
            
        )

    
        val dfWithoutSchema = spark.createDataFrame(
            spark.sparkContext.parallelize(data),
            schema
        )
        
//        val rdd = spark.sparkContext.parallelize(
//            Seq(
//                (-2054210684, my_schema1))
//            //                (-2052218679, list2))
//            //            ("test", Array(1.5, 0.5, 0.9, 3.7)),
//            //            ("choose", Array(8.0, 2.9, 9.1, 2.5))
//        )

//        val dfWithoutSchema: DataFrame = spark.createDataFrame(rdd).toDF("user", "recommends")
//        //    val dfWithoutSchema = spark.createDataset(rdd).collect()
        dfWithoutSchema.show(10, false)
        import spark.implicits._
        val resListBf = scala.collection.mutable.ListBuffer[RecommUserData]()
//        dfWithoutSchema.foreachPartition((rdd: Iterator[Row]) => {
//            rdd.foreach(x => {
//                println(x.getAs[Row]("user"))
//                val user = (x.getAs[Int]("user"))
//                println(x.getAs[Row]("recommendations"))
//                println(s">>>>>>>>>>>>x: $x")
//                val recommendations = x.getAs[mutable.WrappedArray[GenericRowWithSchema]]("recommendations")
//                println(s">>>>>>>>>>>>recommendations: $recommendations")
//
//                recommendations.foreach { array =>
//                    println(s">>>>>>>>>>>>array: $array")
//                    val item = array.getAs[Int]("item")
//                    val prediction = array.getAs[Double]("rating")
//
//                    resListBf.append(RecommUserData(user.toString, item.toInt, prediction))
//                }
//            })
//            resListBf.foreach(x =>
//                println(s">>>>>>>>>>>>>x: $x")
//            )
//        })
        
        // mapPartition 로직
        val aaa = dfWithoutSchema.mapPartitions( rdd => {
            rdd.map(x => {
                println(x.getAs[Row]("user"))
                val user = (x.getAs[Int]("user"))
                println(x.getAs[Row]("recommendations"))
                println(s">>>>>>>>>>>>x: $x")
                val recommendations = x.getAs[mutable.WrappedArray[GenericRowWithSchema]]("recommendations")
                println(s">>>>>>>>>>>>recommendations: $recommendations")
            
                var item = 0
                var prediction = 0.0
                recommendations.map { array =>
                    println(s">>>>>>>>>>>>array: $array")
                    item = array.getAs[Int]("item")
                    prediction = array.getAs[Double]("rating")
                
//                    resListBf.append(RecommUserData(user.toString, item.toInt, prediction))
                    (item.toInt, prediction)
                }
                (user.toString, item.toInt, prediction)
            })
//            resListBf.foreach(x =>
//                println(s">>>>>>>>>>>>>x: $x")
//            )
        
        }).toDF("user", "item", "prediction")
    
        aaa.show(10)

        case class RecommUserData(user: String, item: Int, prediction: Double)
    }
}
