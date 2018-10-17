package com.kthcorp.daisy.picks.junit

import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.streaming.{Durations, StreamingContext, Time}

object SteamingTest {
    
    val log = Logger.getRootLogger()
    
    private val SLIDE_INTERVAL_SECOND = Durations.seconds(10)
    private val WINDOW_INTERVAL_SECOND = Durations.seconds(30)
    case class Word(word: String)
    
    def streamingWindow(spark: SparkSession): Unit = {
        val ssc = new StreamingContext(spark.sparkContext, SLIDE_INTERVAL_SECOND)
        val testStream = ssc.socketTextStream("localhost", 9999).window(WINDOW_INTERVAL_SECOND, SLIDE_INTERVAL_SECOND)
    
        import spark.implicits._
        testStream.flatMap(f => f.split(" ")).foreachRDD((testRDD, time) => {
            log.info(s"time :: ${getDateFormat(time)}")
        
            val df: DataFrame = testRDD.toDF("word")
            val ds: Dataset[Word] = df.as[Word]
            
            df.createOrReplaceTempView("word_table")
            spark.sql("select word, count(*) from word_table group by word").show()
        })
    
        ssc.start()
        ssc.awaitTermination()
    }
    
    def getDateFormat(time: Time): String = {
        val sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        sf.format(time.milliseconds)
    }
    
    // Running Netcat server
    // terminal cmd -> nc -lk 9999
    def main(args: Array[String]): Unit = {
        try {
            Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
            Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
        
            val conf = new SparkConf().setMaster("local[2]").setAppName("test")
            val spark: SparkSession = SparkSession.builder().config(conf)
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate()

            streamingWindow(spark)
        } catch {
            case e: Exception => log.error("", e)
        } finally {
        
        }
    }
}
