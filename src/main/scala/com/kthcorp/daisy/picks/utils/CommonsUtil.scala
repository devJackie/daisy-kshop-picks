package com.kthcorp.daisy.picks.utils

import java.nio.charset.CodingErrorAction
import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

import scala.beans.BeanProperty
import scala.io.Codec

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
    * yaml config read
    * @return
    */
  def getYaml(profiles: String): java.util.Map[String, java.util.Map[String, String]] = {
    val stream = profiles match {
      case _ if profiles == "LOCAL" => getClass.getResourceAsStream("/picks-common-local.yaml")
      case _ if profiles == "DEVELOP" => getClass.getResourceAsStream("/picks-common-dev.yaml")
      case _ if profiles == "PRODUCTION" => getClass.getResourceAsStream("/picks-common.yaml")
      case _ => getClass.getResourceAsStream("/picks-common.yaml")
    }
    val yml = new Yaml().load(stream).asInstanceOf[java.util.Map[String, java.util.Map[String, String]]]
    yml
  }

//  def getYaml3(profiles: String): java.util.Map[String, java.util.Map[String, String]] = {
//	  // https://stackoverflow.com/questions/13625024/how-to-read-a-text-file-with-mixed-encodings-in-scala-or-java
//	  implicit val codec = Codec("UTF-8")
//	  codec.onMalformedInput(CodingErrorAction.REPLACE)
//	  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
//    val source = profiles match {
//      case _ if profiles == "LOCAL" => scala.io.Source.fromURL(getClass.getResource("/common-local.yaml")).bufferedReader()
//      case _ if profiles == "DEVELOP" => scala.io.Source.fromURL(getClass.getResource("/common-dev.yaml")).bufferedReader()
//      case _ if profiles == "PRODUCTION" => scala.io.Source.fromURL(getClass.getResource("/common.yaml")).bufferedReader()
//      case _ => scala.io.Source.fromURL(getClass.getResource("/common.yaml")).bufferedReader()
////      case _ => getClass.getResourceAsStream("/common.yaml")
//    }
//	  val yml = new Yaml()
//	  val resYml = yml.load(source).asInstanceOf[java.util.Map[String, java.util.Map[String, String]]]
//	  resYml
//  }
//
//	def getYaml2(profiles: String): java.util.Map[String, String] = {
//		implicit val codec = Codec("UTF-8")
//		codec.onMalformedInput(CodingErrorAction.REPLACE)
//		codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
//		val source = profiles match {
//			case _ if profiles == "LOCAL" => scala.io.Source.fromURL(getClass.getResource("/common-local.yaml")).bufferedReader()
//			case _ if profiles == "DEVELOP" => scala.io.Source.fromURL(getClass.getResource("/common-dev.yaml")).bufferedReader()
//			case _ if profiles == "PRODUCTION" => scala.io.Source.fromURL(getClass.getResource("/common.yaml")).bufferedReader()
//			case _ => scala.io.Source.fromURL(getClass.getResource("/common.yaml")).bufferedReader()
//			//      case _ => getClass.getResourceAsStream("/common.yaml")
//		}
//		val yaml = new Yaml()
//		val config = yaml.load(source).asInstanceOf[java.util.Map[String, String]]
//		config
//	}

//  def getYaml(profiles: String): CustYamls = {
////    implicit val codec = Codec("UTF-8")
////    codec.onMalformedInput(CodingErrorAction.REPLACE)
////    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
//    val stream = profiles match {
//      case _ if profiles == "LOCAL" => getClass.getResourceAsStream("/picks-common-local.yaml")
//      case _ if profiles == "DEVELOP" => getClass.getResourceAsStream("/picks-common-dev.yaml")
//      case _ if profiles == "PRODUCTION" => getClass.getResourceAsStream("/picks-common.yaml")
//      case _ => getClass.getResourceAsStream("/picks-common.yaml")
//    }
//    val yaml = new Yaml(new Constructor(classOf[CustYamls]))
//    val config = yaml.load(stream).asInstanceOf[CustYamls]
//    config
//  }

  /**
    * 시간 포맷 변경
    * @param time
    * @param timeFormat
    * @return
    */
  def getConvertTime(time: org.apache.spark.streaming.Time, timeFormat: String): String = {
    val dateTime = new DateTime(time.milliseconds)
    val formatter = DateTimeFormat.forPattern(timeFormat)
    formatter.print(dateTime)
  }

  /**
    * current get hour
    * @return
    */
  def getTimeForHour(): String = {
    // 현재일자 날짜 생성
    val yyyyMMddHHmmss = DateTime.now().toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
    val dtf = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    dtf.parseDateTime(yyyyMMddHHmmss).getHourOfDay.toString
  }

  /**
    * current get hour
    * @param time
    * @return
    */
  def getTimeForHour(time: org.apache.spark.streaming.Time): String = {
    val dateTime = new DateTime(time.milliseconds)
    dateTime.getHourOfDay.toString
  }

  /**
    * current get hour
    * @param time
    * @return
    */
  def getTimeForMinute(time: org.apache.spark.streaming.Time): String = {
    val dateTime = new DateTime(time.milliseconds)
    dateTime.getMinuteOfHour.toString
  }

  /**
    * 포맷 변환
    * @param date
    * @param fromFormat
    * @param toFormat
    * @return
    */
  def getConvertDateFormat(date: String, fromFormat: String, toFormat: String): String = {
    var convertDate: String = ""
    if (!date.isEmpty) {
      var dateFormat = new SimpleDateFormat(fromFormat)
      val varDate = dateFormat.parse(date)
      dateFormat = new SimpleDateFormat(toFormat)
      convertDate = dateFormat.format(varDate)
    }
    convertDate
  }

  /**
    * 포맷 변환
    * @param date
    * @param fromFormat
    * @param toFormat
    * @return
    */
  def getConvertDateFormat(date: String, locale: Locale, fromFormat: String, toFormat: String): String = {
    var convertDate: String = ""
    if (!date.isEmpty) {
      var dateFormat = new SimpleDateFormat(fromFormat, locale)
      val varDate = dateFormat.parse(date)
      dateFormat = new SimpleDateFormat(toFormat)
      convertDate = dateFormat.format(varDate)
    }
    convertDate
  }

  def getTestOptionConvertDateFormat(date: String, fromFormat: String, toFormat: String): Option[String] = {
    var convertDate: Option[String] = None
    if (!date.isEmpty) {
      var dateFormat = new SimpleDateFormat(fromFormat)
      val varDate = dateFormat.parse(date)
      dateFormat = new SimpleDateFormat(toFormat)
      convertDate = Option(dateFormat.format(varDate))
    }
    convertDate
  }

  /**
    * 소요시간 조회
    * @param name
    * @param block
    * @tparam R
    * @return
    */
  def elapsedTime[R](name: String, block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis()
    //    log(s"$name Elapsed time: ($t1 - $t0) ms")
    result
  }

  /**
    * null
    * @param str
    * @return
    */
  def getIsNull(str: String): String = {
    //		if (str == null || str.isEmpty) return ""
    //		str
    str match {
      case _ if (str == null || str.isEmpty || str.length == 0) => null
      case _ => str
    }
  }

  /**
    * null to ""
    * @param str
    * @return
    */
  def getIsNullStr(str: String): String = {
    //		if (str == null || str.isEmpty) return ""
    //		str
    str match {
      case _ if (str == null || str.isEmpty || str.length == 0) => ""
      case _ => str
    }
  }

  /**
    * null to None
    * @param str
    * @return
    */
  def getIsNone(str: String): Option[String] = {
    //		if (str == null || str.isEmpty) return ""
    //		str
    str match {
      case _ if (str == null || str.isEmpty || str.length == 0) => None
      case _ => Some(str)
    }
  }

  def getIsNullStatus(str: String): Boolean = {
    str match {
      case _ if (str == null || str.isEmpty || str.length == 0) => true
      case _ => false
    }
  }
}
