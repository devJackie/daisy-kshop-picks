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
    
        val hash17 = "201806186819".hashCode //-1775261572
        println(hash17.toInt)
    
        val hash18 = "201610044826".hashCode //-1774947186
        println(hash18.toInt)
    
        val hash19 = "201801921450".hashCode //-1694473283
        println(hash19.toInt)
    
        val hash20 = "201605536436".hashCode //1062038887
        println(hash20.toInt)
    
        val hash21 = "201609908404".hashCode //428891843
        println(hash21.toInt)
    
        val hash22 = "201509997338".hashCode //-2050339956
        println(hash22.toInt)
    
        val hash23 = "201602373784".hashCode //-1654122711
        println(hash23.toInt)
    
        val hash24 = "201701496919".hashCode //-23546283
        println(hash24.toInt)
        
        val hash25 = "201609930888".hashCode //431428174
        println(hash25.toInt)
    
        val hash26 = "201703633279".hashCode //1803082341
        println(hash26.toInt)
    
        val hash27 = "201709365941".hashCode //-1544880847
        println(hash27.toInt)
    
        val hash28 = "201602298696".hashCode //-1680756793
        println(hash28.toInt)
    
        val hash29 = "201603470983".hashCode //-738077331
        println(hash29.toInt)
    
        val hash30 = "201505768395".hashCode //-1365386275
        println(hash30.toInt)
    
        val hash31 = "201709357546".hashCode //-1545748625
        println(hash31.toInt)
    
        val hash32 = "201606666559".hashCode //1980943308
        println(hash32.toInt)
    
        val hash33 = "201807523058".hashCode //-778879351
        println(hash33.toInt)
    
        val hash34 = "201509960580".hashCode //-2053316987 |201509960580|408464|30   |
        println(hash34.toInt)
    
        val hash35 = "201505786308".hashCode //-1363599091
        println(hash35.toInt)
    
        val hash36 = "201507883010".hashCode //439945189
        println(hash36.toInt)
    
        val hash37 = "201807300492".hashCode //-838070106 // 구매 30건
        println(hash37.toInt)
        
        val hash38 = "201806928711".hashCode //-1551710877 // 구매 6건
        println(hash38.toInt)
    
        val hash39 = "201705986186".hashCode //-626284095 // 구매 7건
        println(hash39.toInt)
    
        val hash40 = "201712864124".hashCode //-1576520766 // 구매 8건
        println(hash40.toInt)
    
        val hash41 = "201705991787".hashCode //-625503762 // 구매 8건
        println(hash41.toInt)
    
        val hash42 = "201507911980".hashCode //462058977 // 구매 10
        println(hash42.toInt)
    
    
        val hash43 = "201603485599".hashCode //-737008662 // 구매 20
        println(hash43.toInt)
    
        val hash44 = "201206141718".hashCode //475686613 // 구매 1
        println(hash44.toInt)
    
        val hash45 = "201811020000".hashCode //-209359097 // 구매 1
        println(hash45.toInt)
        
        
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
