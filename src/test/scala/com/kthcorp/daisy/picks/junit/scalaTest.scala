package com.kthcorp.daisy.picks.junit

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object scalaTest extends App{
    
    def matchTest(x: Int): String = x match {
        case 1 => "one"
        case 2 => "two"
        case _ => "many"
    }
    println(matchTest(2))
    
    for (i <- 0 to 3; if i == 0)
        println(i)
    
    val a = for (i <- 0 to 3) yield i
    println(a)
    
    val list = List(1, 2, 3)
    val aa = for {
        i <- list
    } yield {
        i > 2
    }
    println(aa)
    
    val intList = ListBuffer[Int]()
    intList.append(1)
    intList.append(2)
    
    val intList1 = ListBuffer[mutable.HashMap[String,String]]()
    val maps11 = mutable.HashMap[String,String]()
    maps11.put("AAA", "A")
    intList1.append(maps11)
    
    val maps22 = mutable.HashMap[String,String]()
    maps22.put("BBB", "B")
    intList1.append(maps22)
    
//    val bbb = aaa.map(x => x)
//    val bbbb = intList1.map { x => x}.map{y => y.get("AAA")}.foreach(println)
//    val bbbb = intList1.map(x => x).apply(1).map{ case(_,y) => y}.foreach(println)
    val bbbb = intList1.map(x => x).apply(0).map{ case(x,y) => y}.foreach(println)
    
    val maps1 = mutable.HashMap[Int, mutable.HashSet[Int]]()
    maps1.put(3, mutable.HashSet(1))
    maps1.put(4, mutable.HashSet(2))
//    val ccc = maps1.map { case(x,y) => x}.foreach(println)
    val ccc = maps1.map {x => x._1}.foreach(println)
    
//    val bbb = aaa.map(x => x + x)
    val intResList = intList.reduce(_+_)
    println(s"intResList = $intResList")
    
    case class testCl (aId: String, bId: String, cId: String, dId: String)
    
    val listbf = scala.collection.immutable.List[String]()
    val strbf = new StringBuilder
    strbf.append("a")
    strbf += 'b'
    strbf += 'c'
    
    println(listbf)
    println(listbf.addString(strbf))
    
    
    val strList = ListBuffer[String]()
    strList.append("1")
    strList.append("2")
    strList.append("3")
    
    var bbb: ListBuffer[Int] = ListBuffer(strList.map(_.toString.toInt).reduce(_ + _))
    println(s"bbb = $bbb")
    
    
//    Seq(Seq(1,2,5,7), Seq(2,4,6,8)).foreach {
//        case Seq(_, 3, _*) => println("홀수")
//        case Seq(2, _*) => println("짝수")
//    }
    val arr = 2
    
    testCl("1", "2", "3", "4")
    val testClList = ListBuffer[testCl]()
    
//    testClList.filter { case (aId, cId, bId, dId) => println(s"$cId") }

//    val ccc = strList.



//    val a = { println("Hello, world #1"); 10 }
//
//    println(a)
//
//    def main(args: Array[String]) {
//        println("Hello, world #3!")
//    }
//
//    println("Hello, world #2!")
    
    val resultSummaryData = scala.collection.mutable.ListBuffer[String]()
    
    def setKeyword(
                      resultSummaryData: scala.collection.mutable.ListBuffer[String]
                  ) = {
        resultSummaryData.append("a")
    }
    
}
