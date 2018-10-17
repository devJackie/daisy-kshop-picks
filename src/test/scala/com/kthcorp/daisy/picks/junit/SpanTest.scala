package com.kthcorp.daisy.picks.junit

import scala.collection.mutable

object SpanTest {
    def main(args: Array[String]): Unit = {
    
        val strbf = scala.collection.mutable.ListBuffer[String]()
        val line = "19306,[금수랑] 어리연 귀걸이"
        strbf.append(line)
        val line1 = "19306,[금수랑] 어리연 귀걸이"
        strbf.append(line1)
        val aaa = strbf.flatMap{ line =>
            val (id, name) = line.span(_ != ',')
            println (id, name)
            var m = new mutable.HashMap[String, String]()
            m.put("id", id)
            m.put("name", name)
            println(m)
//            val aa = scala.collection.mutable.ListBuffer[String]()
//            aa.append(id)
//            aa.append(name)
            Some(id, name)
        }
        println(aaa)
    
//        val strbf1 = scala.collection.mutable.ListBuffer[String]()
//        val line3 = "19306,[금수랑] 어리연 귀걸이"
//        strbf1.append(line3)
//        val line4 = "19306,[금수랑] 어리연, 귀걸이"
//        strbf1.append(line4)
//        val bbb = strbf1.flatMap{ line =>
//            val splitted = line.split(",")
//            val (id, name) = (splitted(0), splitted(1))
//            println (id, name)
//            var m = new mutable.HashMap[String, String]()
//            m.put("id", id)
//            m.put("name", name)
//            println(m)
//            //            val aa = scala.collection.mutable.ListBuffer[String]()
//            //            aa.append(id)
//            //            aa.append(name)
//            Some(id, name)
//        }
//        println(bbb)

        val list = List(6, 5, -1, 0, 4, 10, 30)
        println(list.span(x => x > 5))   // (List(19),List(5, -1, 0, 4, 10, 30))


//        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
//        sparkConf.set("spark.sql.crossJoin.enabled", "true")
//        sparkConf.set("spark.executor.memory", "4g")
//        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
//
//        val base = "hdfs://localhost/user/devjackie/dj/"
//        val rawItemData: Dataset[String] = spark.read.textFile(base + "kakao_item_item_nm_data.csv").repartition(10)
//
//        import spark.implicits._
//        val itemData = rawItemData.flatMap { line =>
////            val (id, name) = line.span(_ != '\t')
//            val splitted = line.split(",")
//            val (id, name) = (splitted(0), splitted(1))
//            if (name.isEmpty) {
//                None
//            } else {
//                try {
//                    Some((id, name.trim))
//                } catch {
//                    case _: NumberFormatException => None
//                }
//            }
//        }.toDF("id", "name")
//
//        itemData.show(10)
    }
}
