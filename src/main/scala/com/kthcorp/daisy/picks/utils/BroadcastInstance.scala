package com.kthcorp.daisy.picks.utils

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable

/**
  * spark broadcast instance
  */
object BroadcastInstance extends Serializable {
    @transient lazy val log = Logger.getRootLogger()
    @volatile private var broadCastHashUserData:  Broadcast[scala.collection.Map[Int,String]] = null
    

    def getBroadCastHashUserData(sc: SparkContext, spark: SparkSession, hashUser: scala.collection.Map[Int,String]): Broadcast[scala.collection.Map[Int,String]] = {
        synchronized {
            try {
                broadCastHashUserData = sc.broadcast(hashUser)
            } catch {
                case e: Exception => log.error("", e)
            }
        }
        broadCastHashUserData
    }
    
    def getIsNull(str: String): String = {
        if (str == "\\N" || str == null || str.isEmpty) return null
        str
    }

    def getDateToMillis(str: String): Long = {
        val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        var result = 0L
        try {
            result = format.parse(str).getTime / 1000
        } catch {
            case e: Exception => println(e.getMessage)
        }
        result
    }
}
