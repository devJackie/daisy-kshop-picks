package com.kthcorp.daisy.picks.utils

import java.util.Calendar

import org.yaml.snakeyaml.Yaml

/**
  * Common Utils
  */
object Commons {
  
  /**
    * 시간 포맷 조회
    * @param Time
    * @param timeFormat
    * @return
    */
  def getTimeFormat(Time: org.apache.spark.streaming.Time, timeFormat: String): String = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(Time.milliseconds)
    val format = new java.text.SimpleDateFormat(timeFormat)
    format.format(cal.getTime)
  }
  
  /**
    * yaml
    * @return
    */
  def getYaml(): java.util.Map[String, java.util.Map[String, String]] = {
      val io = scala.io.Source.fromURL(getClass.getResource("/config.yaml")).bufferedReader()
      val config = new Yaml().load(io).asInstanceOf[java.util.Map[String, java.util.Map[String, String]]]

      config
  }
  
  /**
    * 소요시간 조회
    * @param name
    * @param block
    * @tparam R
    * @return
    */
  def time[R](name: String, block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis()
    println(name + " Elapsed time: " + (t1 - t0) + " ms")
    result
  }
  
}
