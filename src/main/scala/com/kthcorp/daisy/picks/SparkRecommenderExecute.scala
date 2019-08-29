package com.kthcorp.daisy.picks

import java.net.URI

import com.kthcorp.daisy.picks.utils.{BroadcastInstance, CommonsUtil, HdfsUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
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

case class PreUserInfo(oriUser: String, user: Int, item: Int, count: Int)

case class OriUserInfo(user: Int, item: Int, count: Int)

case class OriMappingUserInfo(user: String, item: Int, count: Int)

/**
	* create by devjackie on 2018.10.17
	*/
class SparkRecommenderExecute(private val spark: SparkSession, private val profiles: String, private val p_yymmdd: String, private val p_hh: String) extends Serializable {

	@transient lazy val log = Logger.getRootLogger()
	Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
	Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)

	import spark.implicits._

	def preparation(
		               rawUserItemData: Dataset[String]): DataFrame = {

		val newRawUserItemDataDF = rawUserItemData.map { lines =>
			lines.split("\001") match {
				case Array(user, item, count) => PreUserInfo(user.toString, user.hashCode, item.toInt, count.toInt)
				case Array(user, item, _*) => PreUserInfo(user.toString, user.hashCode, item.toInt, 0)
				case Array(user, _*) => PreUserInfo(user.toString, user.hashCode, 0, 0)
			}
		}.toDF("oriUser", "user", "item", "count")
//		newRawUserItemDataDF.createOrReplaceTempView("temp")
//		spark.sql("select b.oriUser, b.count, b.cnt from (select a.oriUser, a.count, a.cnt from (select oriUser, count, count(1) as cnt from temp group by oriUser, count) a group by a.oriUser, a.count, a.cnt having a.cnt <= 3 ) b where b.count > 1 ").show(false)
//		spark.sql("select oriUser, item, count from temp where oriUser = '201602373784'  ").show(false)
		newRawUserItemDataDF
	}

//		def preparation(
//			               rawUserItemData: DataFrame): DataFrame = {
//			rawUserItemData.map { lines =>
//				PreUserInfo(lines.getAs("user"), lines.getAs("user").hashCode, lines.getAs("item"), lines.getAs("count"))
//			}.toDF("oriUser", "user", "item", "count")
//		}

//	def preparation(
//									rawUserItemData: DataFrame): DataFrame = {
//
//		rawUserItemData.createOrReplaceTempView("temp")
//		rawUserItemData.select("select user, item, count from temp").toDF()
//		var df = spark.sql("select word, count(*) from wordtable group by word")
//
//		val newRawUserItemDataDF = rawUserItemData.map { lines =>
//
//		}.toDF("oriUser", "user", "item", "count")
//		newRawUserItemDataDF
//	}

	def buildHashUserMap(rawUserItemData: Dataset[String]): scala.collection.Map[Int, String] = {
		rawUserItemData.flatMap { lines =>
			lines.split("\001") match {
				case Array(user, _*) => Some((user.hashCode, user.toString))
				case Array(_*) => None
			}
		}.collect().toMap
	}

	def model(
		         rawUserItemData: Dataset[String],
		         rawItemData: Dataset[String],
		         trainData: DataFrame): Unit = {

		trainData.cache()

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

		val userID = -1775785176
		//        val userID = -777771416 // prediction 에 지수로 표시되는 데이터가 존재
		val oriUserID = "201610037428"
		val existingItemIDs = trainData.
			filter($"user" === userID).
			select("item").as[Int].collect()

		val itemByID = buildItemByID(rawItemData)

		log.info(s">> item(${userID}) 의 아이템 정보 => item id, name")
		itemByID.show(10, false)

		log.info(s">> user(${userID}) 구매 정보 => item id, name")
		itemByID.filter($"id" isin (existingItemIDs: _*)).show(1000, false)

		log.info(s">> user(${userID}) 추천 정보 => item, prediction")
		val topRecommendations = makeRecommendations(model, userID, 5)
		topRecommendations.show()

		val recommendedItemIDs = topRecommendations.select("item").as[Int].collect()

		itemByID.filter($"id" isin (recommendedItemIDs: _*)).show()

		log.info(s">> user(${userID}) recommendations")
		val recommData = model.recommendForAllUsers(10)
		recommData.filter($"id" isin (userID)).show(false)
		//        recommData.filter($"id" isin (-777771416)).show(false)

		// user, hashUser Map 데이터 생성
		val bcHashUser: Broadcast[Map[Int, String]] = BroadcastInstance.getBroadCastHashUserData(spark.sparkContext, spark, buildHashUserMap(rawUserItemData))
		//        broadcastHashUser.value.foreach(x => println(s"# x: >> $x"))
		log.info(s"# broadcastHashUser value size : ${bcHashUser.value.size}")
		log.info(s"# broadcastHashUser value head : ${bcHashUser.value.head}")

		// dataset 을 dataframe 로 변환
		val oriUserItemDataDF = rawUserItemData.map { line =>
			// 원천 data 예외처리
			line.split("\001") match {
				case Array(user, item, count) => OriUserInfo(user.hashCode, item.toInt, count.toInt)
				case Array(user, item, _*) => OriUserInfo(user.hashCode, item.toInt, 0)
				case Array(user, _*) => OriUserInfo(user.hashCode, 0, 0)
			}
		}.toDF("user", "item", "count")
		//            .cache()
		//        oriUserItemDataDF.show(10, false)
		log.info(s"# oriUserItemDataDF show!")
		oriUserItemDataDF.filter($"user" isin (userID)).show(50, false)
		//        oriUserItemDataDF.filter($"user" isin (-777771416)).show(50, false)

		// 기존 dataframe 의 user, recommendations -> 신규 dataframe 의 user, item, prediction 으로 변환
		val recommDF = recommData.select($"user", explode($"recommendations")).select($"user", $"col.item", $"col.rating" as "prediction").toDF().cache()
		log.info(s"# recommDF show!")
		recommDF.show(10, false)

		// 위의 explode 쓴 쿼리 로직을 mapPartition 로직으로 교체할 수 있음
		// mapPartition
		//        val recommDF = recommData.mapPartitions( rdd => {
		//            //Iterator[(Int, Int, Double)]
		//            //Iterator[WrappedArray[(Int, Int, Double)]]
		//            rdd.map( x => {
		//                val user = (x.getAs[Int]("user"))
		//                val recommendations = x.getAs[mutable.WrappedArray[GenericRowWithSchema]]("recommendations")
		//
		//                var item = 0
		//                var prediction = 0.0
		//                recommendations.map( array => {
		//                    item = array.getAs[Int]("item")
		//                    prediction = array.getAs[Float]("rating")
		//                    (item, prediction)
		//                }).map( x => {
		//                    (user, x._1, x._2)
		//                })
		//            }).flatten
		//        }).toDF("user", "item", "prediction")

		log.info(s"# ${userID} recommDF show!")
		recommDF.filter($"user" isin (userID)).show(false)
		//        recommDF.filter($"user" isin (-777771416)).show(false)

		// 1-1 user 구매내역정보와 user 추천결과정보를 매핑
		val joinRecommDF = recommDF.select('user as "user", 'item as "item", 'prediction as "prediction")
			.join(oriUserItemDataDF.select('user as "oriUser", 'item as "oriItem", 'count as "oriCount")
				, ($"user" === $"oriUser") && ($"item" === $"oriItem")
				, "inner").cache()
		log.info(s"# joinRecommDF show!")
		//        joinRecommDF.show(10, false)
		joinRecommDF.filter($"user" isin (userID)).show(50, false)
		//        joinRecommDF.filter($"user" isin (-777771416)).show(50, false)


		log.info(s"# recommDF2 show!")
		//        filterRecommDF.show(10,false)
		recommDF.filter($"user" isin (userID)).show(50, false)
		//        recommDF.filter($"user" isin (-777771416)).show(50,false)
		// 1-2 user 추천결과정보에서 1-1 의 매핑 정보를 제외
		// user 마다 기존에 구매했던 상품 제외
		val filterRecommDF = recommDF.select('user as "user", 'item as "item", 'prediction as "prediction")
			.except(joinRecommDF.select('user as "user", 'item as "item", 'prediction as "prediction")).cache()
		log.info(s"# recommDF3 show!")
		//        filterRecommDF.show(10,false)
		recommDF.filter($"user" isin (userID)).show(50, false)
		//        recommDF.filter($"user" isin (-777771416)).show(50,false)
		log.info(s"# filterRecommDF show!")
		filterRecommDF.filter($"user" isin (userID)).show(50, false)
		//        filterRecommDF.filter($"user" isin (-777771416)).show(50,false)

		// hashCode 하기전 user ID 로 다시 변환한다
		val finalRecommDF = filterRecommDF.mapPartitions(rdd => {
			rdd.map(x => {
				val user = (x.getAs[Int]("user"))
				val item = (x.getAs[Int]("item"))
				val prediction = (x.getAs[Float]("prediction"))
				val findOriUser = bcHashUser.value.getOrElse(user, user.toString)
				(findOriUser, item, BigDecimal(prediction).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble) // 지수 표기 제외 - 소수점 2자리까지만
			})
		}).toDF("user", "item", "prediction").cache()
		log.info(s"# ${oriUserID}(${userID}) finalRecommDF show!")
		//        finalRecommDF.show(10, false)
		finalRecommDF.filter($"user" isin (oriUserID)).show(50, false)

		// 최종 추천 결과 hdfs 저장
		//        finalRecommDF.coalesce(1).write
		//            .mode(SaveMode.Overwrite)
		//            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/1/p_yymmdd=" + p_yymmdd)

		// broadcast unpersist 는 자동으로 되지만 확실하게 unpersist 해준다
		oriUserItemDataDF.unpersist()
		recommDF.unpersist()
		joinRecommDF.unpersist()
		filterRecommDF.unpersist()
		bcHashUser.unpersist()
		finalRecommDF.unpersist()
	}

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

	def evaluate(
		            rawUserItemData: Dataset[String],
		            rawItemData: Dataset[String],
		            newRawUserItemData: DataFrame): Unit = {

		val allData = newRawUserItemData.cache()

		val Array(trainData, cvData) = allData.randomSplit(Array(0.9, 0.1))
		trainData.cache()
		cvData.cache()

		val allArtistIDs = allData.select("item").as[Int].distinct().collect()
		val bAllArtistIDs = spark.sparkContext.broadcast(allArtistIDs)

		val mostListenedAUC = areaUnderCurve(cvData, bAllArtistIDs, predictMostListened(trainData))
		println(mostListenedAUC)

		val evaluations =
			for (rank <- Seq(5, 30);
			     regParam <- Seq(1.0, 0.0001);
			     alpha <- Seq(1.0, 40.0))
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
		             allData: DataFrame): Unit = {

		log.info(s"# recommend start")

		allData.cache()

		// model의 추첨 점수가 1 이상인 경우에 대한 설명
		// https://stackoverflow.com/questions/46904078/spark-als-recommendation-system-have-value-prediction-greater-than-1
		val model = new ALS()
			.setSeed(Random.nextLong())
			.setImplicitPrefs(true)
			.setRank(28).setRegParam(1.0).setAlpha(40.0).setMaxIter(20)
			.setUserCol("user").setItemCol("item")
			.setRatingCol("count").setPredictionCol("prediction")
			.setCheckpointInterval(2)
			.setColdStartStrategy("drop") // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
			.fit(allData)

		allData.unpersist(true)

		val recommData = model.recommendForAllUsers(10)
		//        recommData.filter($"id" isin (userID)).show(false)

		// user, hashUser Map 데이터 생성
		val bcHashUser: Broadcast[Map[Int, String]] = BroadcastInstance.getBroadCastHashUserData(spark.sparkContext, spark, buildHashUserMap(rawUserItemData))
		log.info(s"# broadcastHashUser value size : ${bcHashUser.value.size}")
		log.info(s"# broadcastHashUser value head : ${bcHashUser.value.head}")

		// dataset 을 dataframe 로 변환
		val oriUserItemDataDF = rawUserItemData.map { line =>
			// 원천 data 예외처리
			line.split("\001") match {
				case Array(user, item, count) => OriUserInfo(user.hashCode, item.toInt, count.toInt)
				case Array(user, item, _*) => OriUserInfo(user.hashCode, item.toInt, 0)
				case Array(user, _*) => OriUserInfo(user.hashCode, 0, 0)
			}
		}.toDF("user", "item", "count")
		//            .cache()
		oriUserItemDataDF.cache()
		log.info(s"# oriUserItemDataDF show!")
		//        oriUserItemDataDF.show(10, false)
		//        oriUserItemDataDF.filter($"user" isin (userID)).show(50, false)

		// mapPartition
		// 기존 dataframe 의 user, recommendations -> 신규 dataframe 의 user, item, prediction 으로 변환
		//https://stackoverflow.com/questions/42354372/spark-explode-a-dataframe-array-of-structs-and-append-id
		//https://hadoopist.wordpress.com/2016/05/16/how-to-handle-nested-dataarray-of-structures-or-multiple-explodes-in-sparkscala-and-pyspark/
		val recommDF = recommData.select($"user", explode($"recommendations")).select($"user", $"col.item", $"col.rating" as "prediction").toDF()
		//            .cache()
		log.info(s"# recommDF show!")
		recommDF.cache()

		//        recommDF.filter($"user" isin (userID)).show(false)

		// 1-1 user 구매내역정보와 user 추천결과정보를 매핑
		val joinRecommDF = recommDF.select('user as "user", 'item as "item", 'prediction as "prediction")
			.join(oriUserItemDataDF.select('user as "oriUser", 'item as "oriItem", 'count as "oriCount")
				, ($"user" === $"oriUser") && ($"item" === $"oriItem")
				, "inner")
		log.info(s"# joinRecommDF show!")
		//        joinRecommDF.show(10, false)
		//        joinRecommDF.filter($"user" isin (userID)).show(50, false)


		log.info(s"# recommDF2 show!")
		//        filterRecommDF.show(10,false)
		//        recommDF.filter($"user" isin (userID)).show(50,false)
		// 1-2 user 추천결과정보에서 1-1 의 매핑 정보를 제외
		// user 마다 기존에 구매했던 상품 제외
		val filterRecommDF = recommDF.select('user as "user", 'item as "item", 'prediction as "prediction")
			.except(joinRecommDF.select('user as "user", 'item as "item", 'prediction as "prediction"))
		//        log.info(s"# recommDF3 show!")
		//        //        filterRecommDF.show(10,false)
		//        recommDF.filter($"user" isin (userID)).show(50,false)
		log.info(s"# filterRecommDF show!")
		//        filterRecommDF.filter($"user" isin (userID)).show(50,false)

		// hashCode 하기전 user ID 로 다시 변환한다
		val finalRecommDF = filterRecommDF.mapPartitions(rdd => {
			rdd.map(x => {
				val user = (x.getAs[Int]("user"))
				val item = (x.getAs[Int]("item"))
				val prediction = (x.getAs[Float]("prediction"))
				val findOriUser = bcHashUser.value.getOrElse(user, user.toString)
				//                (findOriUser, item, prediction)
				//https://stackoverflow.com/questions/11106886/scala-doubles-and-precision
//				(findOriUser, item, BigDecimal(prediction).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble) // 지수 표기 제외 - 소수점 2자리까지만
				("", findOriUser, item, BigDecimal(prediction).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble, p_yymmdd) // 지수 표기 제외 - 소수점 2자리까지만
				//                (findOriUser, item, "%.2f".format(prediction).toDouble)
			})
		}).toDF("pid", "user", "item", "prediction", "yyyymmdd")

		filterRecommDF.unpersist(false)

		// 최종 추천 결과 hdfs 저장
		log.info(s"# hdfs save start")
//		HdfsUtil.saveAsHdfsForRecomm(finalRecommDF, p_yymmdd)
		HdfsUtil.saveAsHdfsForRecomm(spark, finalRecommDF, profiles, CommonsUtil.getYaml(profiles).get("HDFS").get("MART_RMD_CST_PRD_PRCH_OUT_S"), p_yymmdd)
		log.info(s"# hdfs save end")

		// broadcast unpersist 는 자동으로 되지만 확실하게 unpersist 해준다
		oriUserItemDataDF.unpersist(true)
		recommDF.unpersist(true)
		model.userFactors.unpersist(true)
		model.itemFactors.unpersist(true)
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
		val negativeData = positiveData.select("user", "item").as[(Int, Int)].
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
			select("user", "positivePrediction", "negativePrediction")
		//            .cache()

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