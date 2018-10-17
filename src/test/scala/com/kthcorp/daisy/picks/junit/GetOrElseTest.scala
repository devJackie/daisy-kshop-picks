package com.kthcorp.daisy.picks.junit

object GetOrElseTest {
    def main(args: Array[String]): Unit = {
        val numbers = Map(("one", 1), ("two", 2))
        println(numbers.get("one").toString)
        val ttt = numbers.getOrElse("one", numbers.get("two").toString)
        println(ttt)
    
        val personMap = Map(("솔라",1), ("문별",2), ("휘인",3))
        println(personMap.getOrElse("솔라", 4))
        def findByName(name:String)= personMap.getOrElse(name, 4)
        println(findByName("문별"))
    }
}
