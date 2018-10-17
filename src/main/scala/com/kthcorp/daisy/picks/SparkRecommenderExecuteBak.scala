package com.kthcorp.daisy.picks

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{count, _}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable.ArrayBuffer
import scala.collection.{Map, mutable}
import scala.util.Random

class SparkRecommenderExecuteBak(private val spark: SparkSession) extends Serializable {
    
    @transient lazy val log = Logger.getRootLogger()
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
    
    import spark.implicits._
    def preparation(
                       rawUserItemData: Dataset[String]): DataFrame = {
        rawUserItemData.take(5).foreach(println)
    
        val rawUserItemDataDF = rawUserItemData.map{ line =>
//            val Array(user, item, count) = line.split(",")
            val Array(user, item, count) = line.split(' ')
            val transData = Tuple3(user.hashCode, item.toInt, count.toInt)
            (transData)
        }.toDF("user", "item", "count")
    
//        rawUserItemDataDF.show(5)
//        rawUserItemDataDF.agg(min("user"), max("user"), min("count"), max("count")).show()
//        rawUserItemDataDF.select("user").distinct().count()
        rawUserItemDataDF.groupBy("user").agg(count(lit("1")).as("total")).
            select("user", "total").orderBy(desc("total")).show(20,false)
        rawUserItemDataDF.agg(count(lit("1")).as("total")).
            select("total").distinct().show(false)
        rawUserItemDataDF.groupBy("item").agg(count("user").as("total")).
            select("item", "total").orderBy(desc("total")).show(20,false)
//        rawUserItemDataDF.groupBy("item").agg(count("user").as("total")).
//            select("item", "total").orderBy(desc("total")).show(20,false)
//        433207
        rawUserItemDataDF.createOrReplaceTempView("temp")
//        spark.sql("select oriUser, user, item from  temp where user = '145044192' ").show(25,false)
//        spark.sql("select item, count(user) from temp group by item order by count(user) desc").show(false)
//        spark.sql("select a.user, a.cnt from (select user, count(1) as cnt from temp group by user) a order by a.cnt desc").show(1000,false)
//        spark.sql("select a.user, a.cnt from (select user, count(1) as cnt from temp group by user) a group by a.user, a.cnt having a.cnt > 10").show(false)
        spark.sql("select sum(b.cnt) from (select a.user, a.cnt from (select user, count(1) as cnt from temp group by user) a group by a.user, a.cnt having a.cnt > 5) b").show(false)
        rawUserItemDataDF
    }
    case class UserInfo(user: String, hashUser: Int, item: Int, count: Int)
    
    def preparation1(
                       rawUserItemData: Dataset[String]): DataFrame = {
        rawUserItemData.take(5).foreach(println)
        
//        val rawUserItemDataDF = rawUserItemData.map{ line =>
//            val Array(user, item, count) = line.split(",")
//            //            val Array(user, item, count) = line.split(' ')
//            val transData = Tuple3(user.hashCode, item.toInt, count.toInt)
//            (transData)
//        }.toDF("user", "item", "count")
//
        val userInfos = scala.collection.mutable.Map[String, UserInfo]()
        val rawHashUserItemDataDF = rawUserItemData.map{ line =>
            val Array(user, item, count) = line.split(",")
            //            val Array(user, item, count) = line.split(' ')
            Tuple4(user.toString, user.hashCode, item.toInt, count.toInt)
        }.toDF("oriUser", "user", "item", "count")
        
        //        rawUserItemDataDF.show(5)
        //        rawUserItemDataDF.agg(min("user"), max("user"), min("count"), max("count")).show()
        //        rawUserItemDataDF.select("user").distinct().count()
        rawHashUserItemDataDF.groupBy("oriUser", "user").agg(count(lit("1")).as("total")).
            select("oriUser", "user", "total").where("total between 10 and 20").orderBy(desc("total")).show(20,false)
        rawHashUserItemDataDF.agg(count(lit("1")).as("total")).
            select("total").distinct().show(false)
        rawHashUserItemDataDF.groupBy("item").agg(count("user").as("total")).
            select("item", "total").orderBy(desc("total")).show(20,false)
        //        rawUserItemDataDF.groupBy("item").agg(count("user").as("total")).
        //            select("item", "total").orderBy(desc("total")).show(20,false)
        //        433207
        rawHashUserItemDataDF.createOrReplaceTempView("temp")
        //        spark.sql("select item, count(user) from temp group by item order by count(user) desc").show(false)
        //        spark.sql("select a.user, a.cnt from (select user, count(1) as cnt from temp group by user) a order by a.cnt desc").show(1000,false)
        //        spark.sql("select a.user, a.cnt from (select user, count(1) as cnt from temp group by user) a group by a.user, a.cnt having a.cnt > 10").show(false)
        spark.sql("select sum(b.cnt) from (select a.user, a.cnt from (select user, count(1) as cnt from temp group by user) a group by a.user, a.cnt having a.cnt > 10) b").show(false)
        rawHashUserItemDataDF
    }
    
    def model(
                 rawUserItemData: Dataset[String],
                 rawItemData: Dataset[String],
                 newRawUserItemData: DataFrame): Unit = {
        val bRawItemData = spark.sparkContext.broadcast(rawItemData)
        
        val trainData = newRawUserItemData.cache()
        
        val model = new ALS().
            setSeed(Random.nextLong()).
            setImplicitPrefs(true).
            setRank(10).
            setRegParam(0.01).
            setAlpha(1.0).
            setMaxIter(5).
            setUserCol("user").
            setItemCol("item").
            setRatingCol("count").
            setPredictionCol("prediction").
            fit(trainData)
        
        trainData.unpersist()
        
//        model.userFactors.select("features").show(truncate = false)
    
//        val userID = 505271123
//        val userID = 474046185
//        val userID = 620679653
        val userID = 145044192
        val existingItemIDs = trainData.
            filter($"user" === userID).
            select("item").as[Int].collect()
    
        val itemByID = buildItemByID(rawItemData)
    
        log.info(s">> item(432155) 의 아이템 정보 => item id, name")
//        itemByID.filter($"id" isin (432155)).show()
//        itemByID.filter($"id" isin (432735)).show()
//        itemByID.filter($"id" isin (433207)).show()
        itemByID.show(10, false)
        
        log.info(s">> user(145044192) 구매 정보 => item id, name")
        itemByID.filter($"id" isin (existingItemIDs:_*)).show(1000,false)
    
        log.info(s">> user(145044192) 추천 정보 => item, prediction")
        val topRecommendations = makeRecommendations(model, userID, 5)
        topRecommendations.show()
    
        val recommendedItemIDs = topRecommendations.select("item").as[Int].collect()
    
        itemByID.filter($"id" isin (recommendedItemIDs:_*)).show()
    
        log.info(s">> user(145044192) recommendations")
//        model.recommendForAllUsers(5).filter($"id" isin (505271123)).show(20,false)
//        model.recommendForAllUsers(5).filter($"id" isin (474046185)).show(20,false)
        model.recommendForAllUsers(5).filter($"id" isin (145044192)).show(false)
        val recommData = model.recommendForAllUsers(10)
        recommData.createOrReplaceTempView("tempModelData")
        spark.sql("select count(distinct user) from tempModelData").show(false)
        val recommAllData: DataFrame = spark.sql("select * from tempModelData")
        recommAllData.show(10, false)
//        log.info(recommTotal)
//        log.info(s"Recommend Total : $recommTotal")
    
        // foreachPartition
//        val resListBf = scala.collection.mutable.ListBuffer[RecommUserData]()
//        recommData.foreachPartition((rdd: Iterator[Row]) => {
//            rdd.foreach(x => {
//                val user = (x.getAs[Int]("user"))
//                val recommendations = x.getAs[mutable.WrappedArray[GenericRowWithSchema]]("recommendations")
////                println(s">>>>>>>>>>>>recommendations: $recommendations")
//                recommendations.foreach { structField =>
//                    val item = structField.getAs[Int]("item")
//                    val prediction = structField.getAs[Float]("rating")
//                    resListBf.append(RecommUserData(user.toString, item.toInt, prediction))
//                }
//            })
////            resListBf.foreach(x =>
////                println(s">>>>>>>>>>>>>x: $x")
////            )
//        })
        // mapPartition
        val df = recommData.mapPartitions( rdd => {
            rdd.map(x => {
                val user = (x.getAs[Int]("user"))
                val recommendations = x.getAs[mutable.WrappedArray[GenericRowWithSchema]]("recommendations")
                //                println(s">>>>>>>>>>>>recommendations: $recommendations")
                var item = 0
                var prediction = 0.0f
                recommendations.map{ structField =>
                    item = structField.getAs[Int]("item")
                    prediction = structField.getAs[Float]("rating")
                    (item, prediction)
                }
                (user.toString, item, prediction)
            })
        })
    
        val yyyyMMdd = DateTime.now().toString(DateTimeFormat.forPattern("yyyyMMdd"))
        val formatter = DateTimeFormat.forPattern("yyyyMMdd")
        val currDate = formatter.parseDateTime(yyyyMMdd)
        val p_yymmdd = currDate.minusDays(1).toString(DateTimeFormat.forPattern("yyyyMMdd"))
        df.show(10, false)
        df.coalesce(1).write
            .mode(SaveMode.Overwrite)
            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/1/p_yymmdd=" + p_yymmdd)
    }
    case class RecommUserData(user: String, item: Int, prediction: Double)
    
    def makeRecommendations(model: ALSModel, userID: Int, howMany: Int): DataFrame = {
        val toRecommend = model.itemFactors.
            select($"id".as("item")).
            withColumn("user", lit(userID))
        model.transform(toRecommend).
            select("item", "prediction").
            orderBy($"prediction".desc).
            limit(howMany)
    }
    
    def buildItemByID(rawItemData: Dataset[String]): DataFrame = {
        rawItemData.flatMap { line =>
//            val (id, name) = line.span(_ != '\t')
            val splitted = line.split(",")
            val (id, name) = (splitted(0), splitted(1))
            if (name.isEmpty) {
                None
            } else {
                try {
                    Some((id.toInt, name.trim))
                } catch {
                    case _: NumberFormatException => None
                }
            }
        }.toDF("id", "name")
    }
    
    def buildCounts(
                       rawUserItemData: Dataset[String],
                       bArtistAlias: Broadcast[Map[Int,Int]]): DataFrame = {
        rawUserItemData.map { line =>
            val Array(userID, itemID, count) = line.split(' ').map(_.toInt)
            val finalArtistID = bArtistAlias.value.getOrElse(itemID, itemID)
            (userID, finalArtistID, count)
        }.toDF("user", "item", "count")
    }
    
    def evaluate(
                    rawUserItemData: Dataset[String],
                    rawItemData: Dataset[String],
                    newRawUserItemData: DataFrame): Unit = {
        
        val bRawItemData = spark.sparkContext.broadcast(rawItemData)
        val allData = newRawUserItemData.cache()
//        allData.show(50,false)
        val Array(trainData, cvData) = allData.randomSplit(Array(0.9, 0.1))
        trainData.cache()
        cvData.cache()
        
        val allArtistIDs = allData.select("item").as[Int].distinct().collect()
        val bAllArtistIDs = spark.sparkContext.broadcast(allArtistIDs)
        
        val mostListenedAUC = areaUnderCurve(cvData, bAllArtistIDs, predictMostListened(trainData))
        println(mostListenedAUC)
        
        val evaluations =
            for (rank     <- Seq(5,  30);
                 regParam <- Seq(1.0, 0.0001);
                 alpha    <- Seq(1.0, 40.0))
//            for (rank     <- Seq(10,  30);
//                 regParam <- Seq(2.0, 10.0);
//                 alpha    <- Seq(70.0, 50.0))
//            for (rank     <- Seq(5, 6, 7, 8, 9, 10, 30);
//                 regParam <- Seq(1.0, 2.0, 3.0, 4.0, 5.0);
//                 alpha    <- Seq(1.0, 2.0, 3.0, 4.0, 10.0))
//            for (rank     <- Seq(2, 20);
//                 regParam <- Seq(0.01, 0.1);
//                 alpha    <- Seq(1.0, 10.0))
//            for (rank     <- Seq(5,  10);
//                 regParam <- Seq(4.0, 0.01);
//                 alpha    <- Seq(1.0, 10.0))
//            for (rank     <- Seq(5,  20);
//                 regParam <- Seq(0.01, 4.0);
//                 alpha    <- Seq(25.0, 40.0))
                yield {
                    val model = new ALS().
                        setSeed(Random.nextLong()).
                        setImplicitPrefs(true).
                        setRank(rank).setRegParam(regParam).
                        setAlpha(alpha).setMaxIter(20).
                        setUserCol("user").setItemCol("item").
                        setRatingCol("count").setPredictionCol("prediction").
                        fit(trainData)
                    
                    val auc = areaUnderCurve(cvData, bAllArtistIDs, model.transform)
                    
                    model.userFactors.unpersist()
                    model.itemFactors.unpersist()
                    
                    (auc, (rank, regParam, alpha))
                }
        
        evaluations.sorted.reverse.foreach(println)
        
        trainData.unpersist()
        cvData.unpersist()
    }
    
    def recommend(
                     rawUserItemData: Dataset[String],
                     rawItemData: Dataset[String],
                     newRawUserItemData: DataFrame): Unit = {
        
        val bRawItemData = spark.sparkContext.broadcast(rawItemData)
        val allData = newRawUserItemData.cache()
        
        val model = new ALS().
            setSeed(Random.nextLong()).
            setImplicitPrefs(true).
            setRank(10).setRegParam(1.0).setAlpha(40.0).setMaxIter(20).
            setUserCol("user").setItemCol("item").
            setRatingCol("count").setPredictionCol("prediction").
            fit(allData)
        allData.unpersist()
        
//        val userID = 505271123
//        val userID = 474046185
//        val userID = 620679653
        val userID = 145044192
        val topRecommendations = makeRecommendations(model, userID, 5)
        topRecommendations.show(100, false)
        val recommendedItemIDs = topRecommendations.select("item").as[Int].collect()
        val itemByID = buildItemByID(rawItemData)
        itemByID.join(spark.createDataset(recommendedItemIDs).toDF("id2"), $"id" === $"id2", "right_outer").
            select("id2", "name").show(false)
    
        log.info(s">> user(145044192) 최종 추천 => user, recommendations")
        model.recommendForAllUsers(10).filter($"id" isin (145044192)).show(false)
        log.info(s">> user 최종 추천 => user, recommendations")
        model.recommendForAllUsers(10).show(false)
        
//        val recommTotal = model.recommendForAllUsers(10).count()
//        log.info(s"Recommend Total : $recommTotal")
        
        model.userFactors.unpersist()
        model.itemFactors.unpersist()
    }
    
    def areaUnderCurve(
                          positiveData: DataFrame,
                          bAllArtistIDs: Broadcast[Array[Int]],
                          predictFunction: (DataFrame => DataFrame)): Double = {
        
        // What this actually computes is AUC, per user. The result is actually something
        // that might be called "mean AUC".
        
        // Take held-out data as the "positive".
        // Make predictions for each of them, including a numeric score
        val positivePredictions = predictFunction(positiveData.select("user", "item")).
            withColumnRenamed("prediction", "positivePrediction")
        
        // BinaryClassificationMetrics.areaUnderROC is not used here since there are really lots of
        // small AUC problems, and it would be inefficient, when a direct computation is available.
        
        // Create a set of "negative" products for each user. These are randomly chosen
        // from among all of the other artists, excluding those that are "positive" for the user.
        val negativeData = positiveData.select("user", "item").as[(Int,Int)].
            groupByKey { case (user, _) => user }.
            flatMapGroups { case (userID, userIDAndPosArtistIDs) =>
                val random = new Random()
                val posItemIDSet = userIDAndPosArtistIDs.map { case (_, item) => item }.toSet
                val negative = new ArrayBuffer[Int]()
                val allArtistIDs = bAllArtistIDs.value
                var i = 0
                // Make at most one pass over all artists to avoid an infinite loop.
                // Also stop when number of negative equals positive set size
                while (i < allArtistIDs.length && negative.size < posItemIDSet.size) {
                    val itemID = allArtistIDs(random.nextInt(allArtistIDs.length))
                    // Only add new distinct IDs
                    if (!posItemIDSet.contains(itemID)) {
                        negative += itemID
                    }
                    i += 1
                }
                // Return the set with user ID added back
                negative.map(itemID => (userID, itemID))
            }.toDF("user", "item")
        
        // Make predictions on the rest:
        val negativePredictions = predictFunction(negativeData).
            withColumnRenamed("prediction", "negativePrediction")
        
        // Join positive predictions to negative predictions by user, only.
        // This will result in a row for every possible pairing of positive and negative
        // predictions within each user.
        val joinedPredictions = positivePredictions.join(negativePredictions, "user").
            select("user", "positivePrediction", "negativePrediction").cache()
        
        // Count the number of pairs per user
        val allCounts = joinedPredictions.
            groupBy("user").agg(count(lit("1")).as("total")).
            select("user", "total")
        // Count the number of correctly ordered pairs per user
        val correctCounts = joinedPredictions.
            filter($"positivePrediction" > $"negativePrediction").
            groupBy("user").agg(count("user").as("correct")).
            select("user", "correct")
        
        // Combine these, compute their ratio, and average over all users
        val meanAUC = allCounts.join(correctCounts, Seq("user"), "left_outer").
            select($"user", (coalesce($"correct", lit(0)) / $"total").as("auc")).
            agg(mean("auc")).
            as[Double].first()
        
        joinedPredictions.unpersist()
        
        meanAUC
    }
    
    def predictMostListened(train: DataFrame)(allData: DataFrame): DataFrame = {
        val listenCounts = train.groupBy("item").
            agg(sum("count").as("prediction")).
            select("item", "prediction")
        val resAllData = allData.
            join(listenCounts, Seq("item"), "left_outer").
            select("user", "item", "prediction")
        
        resAllData.createOrReplaceTempView("tempAllData")
//        spark.sql("select * from tempAllData order by prediction desc").show(20,false)
        resAllData
    }
}