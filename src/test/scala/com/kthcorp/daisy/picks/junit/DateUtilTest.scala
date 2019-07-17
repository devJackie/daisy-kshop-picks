package com.kthcorp.daisy.picks.junit

import java.util.Locale

import com.kthcorp.daisy.picks.utils.CommonsUtil
import org.joda.time.{DateTime, LocalDate}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object DateUtilTest {

	def main(args: Array[String]): Unit = {

//		println(Try(CommonsUtil.getConvertDateFormat(CommonsUtil.getIsNull(""), "dd-MMM-yy", "yyyy-MM-dd")).get.get)
		println(CommonsUtil.getConvertDateFormat(CommonsUtil.getIsNullStr(""), Locale.ENGLISH, "dd-MMM-yy", "yyyy-MM-dd"))
//		Try(CommonsUtil.getConvertDateFormat("18-JUL-17", "dd-MMM-yy", "yyyy-MM-dd")) match {case Success(s)  => s case Failure(e)  => println(s"FAILURE : ${e}")}
//		println(CommonsUtil.getConvertYyyyMMdd("18-11-27", "yyyy-MM-dd"))

		import java.text.SimpleDateFormat
//		val strDate = "18-7월-17"
//		val strDate = ""
		var dateFormat = new SimpleDateFormat("dd-MMM-yy", Locale.ENGLISH)
		val strDate = "18-JUL-17"
//		var dateFormat = new SimpleDateFormat("dd-MMM-yy")
		try {
			val varDate = dateFormat.parse(strDate)
			dateFormat = new SimpleDateFormat("yyyy-MM-dd")
			println("Date :" + dateFormat.format(varDate))
		} catch {
			case e: Exception =>
				// TODO: handle exception
				e.printStackTrace()
		}

		println(System.currentTimeMillis())
		val dateTime = new DateTime(System.currentTimeMillis())
		val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
		println(formatter.print(dateTime))

		val eventTimestamp: Long = System.currentTimeMillis
		println(eventTimestamp)

		val yyyyMMdd = DateTime.now().toString(DateTimeFormat.forPattern("yyyyMMdd"))
		val formatter1 = DateTimeFormat.forPattern("yyyyMMdd")
		val currDate = formatter1.parseDateTime(yyyyMMdd)
		println(currDate.hourOfDay)

//		val dt = new DateTime().dayOfMonth
//		val fmt = DateTimeFormat.forPattern("HH")

		val yyyyMMddHHmmss = DateTime.now().toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
		val dtf = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
		val dt = dtf.parseDateTime(yyyyMMddHHmmss).getHourOfDay
		println(dt)
//		val dfdf = dt.withHourOfDay(0)
		val fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
		fmt.parseDateTime(yyyyMMddHHmmss)
		val result = fmt.print(dt)
//		val result = new LocalDate(dt).toString(fmt)
		println(result)
	}
}
