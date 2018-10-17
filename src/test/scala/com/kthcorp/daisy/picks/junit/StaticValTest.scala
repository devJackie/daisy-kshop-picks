package com.kthcorp.daisy.picks.junit

object StaticValTest {
    def main(args: Array[String]): Unit = {
        
        val resultSummaryData = scala.collection.mutable.ListBuffer[String]()
        setKeyword(resultSummaryData)
    
        resultSummaryData.foreach(println)
    }
    
    
    def setKeyword(
                      resultSummaryData: scala.collection.mutable.ListBuffer[String]
                  ) = {
        resultSummaryData.append("a")
    }
    
}
