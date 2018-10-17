package com.kthcorp.daisy.picks.junit

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.functions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class UserInfo(user: String, item: Int, prediction: Double)

object DataFrameMapPartitionsTest {
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
    
    @transient lazy val log = Logger.getRootLogger()
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
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
    
//        val rdd = spark.sparkContext.parallelize(
//            Seq(
//                (-2054210684, my_schema))
//            //                (-2052218679, list2))
//            //            ("test", Array(1.5, 0.5, 0.9, 3.7)),
//            //            ("choose", Array(8.0, 2.9, 9.1, 2.5))
//        )
//        val dfWithoutSchema: DataFrame = spark.createDataFrame(rdd).toDF("user", "recommends")
    
    
        //https://jira.mongodb.org/browse/SPARK-132
        val data = Array(
            Row(-2054210684, Array(Row(394924, 0.06045123), Row(394924, 0.06045124))),
            Row(-2054210684, Array(Row(421086, 0.038403966))),
            Row(-2052218679, Array(Row(409836, 0.18487754))),
            Row(-2050393772, Array(Row(407628, 0.3506828)))
        )

//        https://github.com/zalando-incubator/spark-json-schema/blob/master/src/test/scala/org/zalando/spark/jsonschema/SchemaConverterTest.scala
//        StructField("array", ArrayType(StructType(Array(
//            StructField("itemProperty1", StringType, nullable = false),
//            StructField("itemProperty2", DoubleType, nullable = false)
//        )), containsNull = false), nullable = false),
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
        
        dfWithoutSchema.show(10, false)
    
        
//        val df3 = dfWithoutSchema.select(dfWithoutSchema("user"), explode(dfWithoutSchema("recommendations")).alias("recommendations"))
//        df3.show()
        
        import spark.implicits._
        // mapPartition 로직
//        val df1 = dfWithoutSchema.mapPartitions( rdd => {
//            rdd.map(x => {
//                val user = (x.getAs[Int]("user"))
//                val recommendations = x.getAs[mutable.WrappedArray[GenericRowWithSchema]]("recommendations")
//
//                var item = 0
//                var prediction = 0.0
//
//                val list = ListBuffer[UserInfo]()
//                val userInfo: Array[(String, Int, Double)] = recommendations.flatMap { array =>
//                    item = array.getAs[Int]("item")
//                    prediction = array.getAs[Double]("rating")
////                    list.append(UserInfo(user.toString, item, prediction))
//                    Some(user.toString, item, prediction)
//                }.toArray
////                userInfo.toArray match {
////                    case Array(user, item, prediction)  => (user, item, prediction)
////                }
//                userInfo match {
//                    case Array(Tuple3(user, item, prediction))  => Tuple3(user, item, prediction)
//                }
//            })
//        }).toDF("user", "item", "prediction")
////        }).toDF("value")
//        println(s"df1 show!!")
//        df1.show(10, false)


//        val df3 = dfWithoutSchema.select(dfWithoutSchema("user"), explode(dfWithoutSchema("recommendations")).alias("recommendations"))
        dfWithoutSchema.select($"user", explode($"recommendations")).select($"user", $"col.item", $"col.rating" as "prediction").show()
        
        val df2 = dfWithoutSchema.mapPartitions( rdd => {
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
                    (item, prediction)
                }
                (user.toString, item, prediction)
            })
        }).toDF("user", "item", "prediction")
        
        println(s"df2 show!!")
        df2.show(10, false)
        //        df.coalesce(1).rdd.saveAsTextFile("hdfs://localhost/user/devjackie/picks/result/result_6month_20181012.dat")
        val yyyyMMdd = DateTime.now().toString(DateTimeFormat.forPattern("yyyyMMdd"))
        val formatter = DateTimeFormat.forPattern("yyyyMMdd")
        val currDate = formatter.parseDateTime(yyyyMMdd)
        val p_yymmdd = currDate.minusDays(1).toString(DateTimeFormat.forPattern("yyyyMMdd"))
        try {
////            df.write.save("/Users/devjackie/result_6month_20181012.csv")
////            df.coalesce(1).write.mode(SaveMode.Overwrite).option("delimiter", "\u0036").csv("file:/Users/devjackie/result_6month_20181012.csv")
//            df.coalesce(1).write
//                .mode(SaveMode.Overwrite)
//                .format("result_daisy_kshop_picks.csv")
////                .option("delimiter", "\u0001").csv("hdfs://localhost/user/devjackie/picks/result/1")
//                .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/1/p_yymmdd=" + p_yymmdd)
////            df.coalesce(1).rdd.saveAsTextFile("hdfs://localhost/user/devjackie/picks/result/1")
////            df.rdd.map(x=>x.mkString("^A")).saveAsTextFile("file:/home/iot/data/stackOver")
        } catch {
            case e: Exception => log.error("", e)
                System.exit(1)
        } finally {
            log.warn("### end ###")
            System.exit(0)
        }
        
        case class RecommUserData(user: String, item: Int, prediction: Double)
    }
}
