package com.kthcorp.daisy.picks.utils

import java.util.Calendar

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.yaml.snakeyaml.Yaml

/**
  * create by devjackie on 2018.10.17
  */
object CommonsUtil {
  
  /**
    * 현재날짜 조회
    * @return
    */
  def getCurrentDate(): String = {
    // 현재일자 날짜 생성
    val yyyyMMdd = DateTime.now().toString(DateTimeFormat.forPattern("yyyyMMdd"))
    val formatter = DateTimeFormat.forPattern("yyyyMMdd")
    val currDate = formatter.parseDateTime(yyyyMMdd)
    currDate.toString(DateTimeFormat.forPattern("yyyyMMdd"))
  }
  
  /**
    * 어제날짜 조회
    * @return
    */
  def getMinus1DaysDate(): String = {
    // 전일자 날짜 생성
    val yyyyMMdd = DateTime.now().toString(DateTimeFormat.forPattern("yyyyMMdd"))
    val formatter = DateTimeFormat.forPattern("yyyyMMdd")
    val currDate = formatter.parseDateTime(yyyyMMdd)
    currDate.minusDays(1).toString(DateTimeFormat.forPattern("yyyyMMdd"))
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
