package com.kthcorp.daisy.picks.junit

object HashCodeTest {
    def main(args: Array[String]): Unit = {
        
        val hash1 = hashCode("devjackie")
        println(hash1)
    
        val hash2 = hashCode("devjackie")
        println(hash2)
        
        val hash3 = "devjackie".hashCode
        println(hash3)
        
        val hash4 = "BD3E7".hashCode
        println(hash4)
        
        val hash5 = "201206252959".hashCode
        println(hash5.toInt)
    
        val hash6 = "201206661793".hashCode
        println(hash6.toInt)
    
        val hash7 = 1000057.toString.hashCode //1958013459
        println(hash7.toInt)
    
        val hash8 = "201403296261".hashCode
        println(hash8.toInt)
    
        val hash9 = "201612352737".hashCode //86810639
        println(hash9.toInt)
    
        val hash10 = "201604428685".hashCode //145044192
        println(hash10.toInt)
    
        val hash11 = "201603445993".hashCode //-740698908
        println(hash11.toInt)
    
        val hash12 = "201807539623".hashCode //-777771416
        println(hash12.toInt)
    
        val hash13 = "201808439286".hashCode //81099459
        println(hash13.toInt)
    
        val hash14 = "201707206160".hashCode //940930974
        println(hash14.toInt)
    
        val hash15 = "201702579345".hashCode //890823203
        println(hash15.toInt)
    
        val hash16 = "201610037428".hashCode //-1775785176
        println(hash16.toInt)
    }
    
    def hashCode(thiz: String): Int = {
        var res = 0
        var mul = 1 // holds pow(31, length-i-1)
        var i = thiz.length-1
        while (i >= 0) {
            res += thiz.charAt(i) * mul
            mul *= 31
            i -= 1
        }
        res
    }
}
