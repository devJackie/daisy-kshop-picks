package com.kthcorp.daisy.picks

import java.net.URI

import com.kthcorp.daisy.picks.utils.{BroadcastInstance, CommonsUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{count, _}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable.ArrayBuffer
import scala.collection.{Map, mutable}
import scala.util.Random

class TestSparkRecommenderExecute(private val spark: SparkSession) extends Serializable {

	@transient lazy val log = Logger.getRootLogger()
	Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
	Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)

	import spark.implicits._

	def preparation(
		               rawUserItemData: Dataset[String]): DataFrame = {

		val newRawUserItemDataDF = rawUserItemData.map { lines =>
			lines.split(",") match {
				case Array(user, item, count) => PreUserInfo(user.toString, user.hashCode, item.toInt, count.toInt)
				case Array(user, item, _*) => PreUserInfo(user.toString, user.hashCode, item.toInt, 0)
				case Array(user, _*) => PreUserInfo(user.toString, user.hashCode, 0, 0)
			}
		}.toDF("oriUser", "user", "item", "count")
		newRawUserItemDataDF.createOrReplaceTempView("temp")
		spark.sql("select b.oriUser, b.count, b.cnt from (select a.oriUser, a.count, a.cnt from (select oriUser, count, count(1) as cnt from temp group by oriUser, count) a group by a.oriUser, a.count, a.cnt having a.cnt <= 3 ) b where b.count > 1 ").show(false)
		spark.sql("select oriUser, item, count from temp where oriUser = '201602373784'  ").show(false)
		newRawUserItemDataDF
	}

	def buildHashUserMap(rawUserItemData: Dataset[String]): scala.collection.Map[Int, String] = {
		rawUserItemData.flatMap { lines =>
			lines.split(",") match {
				case Array(user, _*) => Some((user.hashCode, user.toString))
				case Array(_*) => None
			}
		}.collect().toMap
	}

	def model(
		         rawUserItemData: Dataset[String],
		         rawItemData: Dataset[String],
		         trainData: DataFrame): Unit = {
		log.info(s"# model start")
		//        val trainData = newRawUserItemData.cache()
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
			line.split(",") match {
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

		// 전일자 날짜 생성
		val yyyyMMdd = DateTime.now().toString(DateTimeFormat.forPattern("yyyyMMdd"))
		val formatter = DateTimeFormat.forPattern("yyyyMMdd")
		val currDate = formatter.parseDateTime(yyyyMMdd)
		val p_yymmdd = currDate.minusDays(1).toString(DateTimeFormat.forPattern("yyyyMMdd"))

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
		             rawItemData: Dataset[String],
		             allData: DataFrame): Unit = {

		log.info(s"# recommend start")

		allData.cache()

		val model = new ALS()
			.setSeed(Random.nextLong())
			.setImplicitPrefs(true)
			.setRank(10).setRegParam(1.0).setAlpha(40.0).setMaxIter(100)
			.setUserCol("user").setItemCol("item")
			.setRatingCol("count").setPredictionCol("prediction")
			.setCheckpointInterval(2)
			.setColdStartStrategy("drop") // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
			.fit(allData)

		allData.unpersist(true)

		//        val userID = 145044192
		//        val userID = -777771416 // prediction 에 지수로 표시되는 데이터가 존재
		//        val userID = 81099459 // 지수가 많음
		//        val userID = 940930974 // 지수가 많음
		//        val userID = 890823203 // 지수가 많음
		//        val oriUserID = "201702579345" // 지수가 많음
		//        val userID = -1775785176 // 추천 예상 평가 점수가 높음 (구매이력 4개)
		//        val oriUserID = "201610037428" // 추천 예상 평가 점수가 높음 (구매이력 4개)
		//        val userID = -1775261572 // 예상 평가 점수 0.4~0.6 (구매이력 1개)
		//        val oriUserID = "201806186819" // 예상 평가 점수 0.4~0.6 (구매이력 1개)
		//        val userID = -1774947186 // 예상 평가 점수 0.6~0.9 (구매이력 5개)
		//        val oriUserID = "201610044826" // 예상 평가 점수 0.6~0.9 (구매이력 5개)
		val userID = -1694473283 // 예상 평가 점수 0.4~0.5 (구매이력 2개)
		val oriUserID = "201801921450" // 예상 평가 점수 0.4~0.5 (구매이력 2개)
		//        val userID = 1062038887 // 예상 평가 점수 0.5~0.7 (구매이력 3개, 남성, 여성 제품 구매)
		//        val oriUserID = "201605536436" // 예상 평가 점수 0.5~0.7 (구매이력 3개, 남성, 여성 제품 구매)
		//        val userID = 428891843 // 예상 평가 점수 0.4~0.6 (구매이력 4개, 구매이력중 value 가 전부 1)
		//        val oriUserID = "201609908404" // 예상 평가 점수 0.4~0.6 (구매이력 4개, 구매이력중 value 가 전부 1)
		//        val userID = -2050339956 // 예상 평가 점수 1이상 (구매이력 16개, 구매이력중 value 가 2이상인게 1개)
		//        val oriUserID = "201509997338" // 예상 평가 점수 1이상 (구매이력 16개, 구매이력중 value 가 2이상인게 1개)
		//        val userID = -1654122711 // 예상 평가 점수 0.7~0.9 (구매이력 3개, 구매이력중 value 가 2이상인게 2개)
		//        val oriUserID = "201602373784" // 예상 평가 점수 0.7~0.9 (구매이력 3개, 구매이력중 value 가 2이상인게 2개)

		allData.createOrReplaceTempView("temp1")
		spark.sql(s"select oriUser, item, count from temp1 where oriUser = '${oriUserID}'  ").show(false)

		val existingItemIDs = allData.
			filter($"user" === userID).
			select("item").as[Int].collect()

		val itemByID = buildItemByID(rawItemData)
		log.info(s">> user(${userID}) 구매 정보 => item id, name")
		itemByID.filter($"id" isin (existingItemIDs: _*)).show(1000, false)

		log.info(s">> user(${userID}) recommendations")
		val recommData = model.recommendForAllUsers(10)
		//        recommData.filter($"id" isin (userID)).show(false)

		// user, hashUser Map 데이터 생성
		val bcHashUser: Broadcast[Map[Int, String]] = BroadcastInstance.getBroadCastHashUserData(spark.sparkContext, spark, buildHashUserMap(rawUserItemData))
		log.info(s"# broadcastHashUser value size : ${bcHashUser.value.size}")
		log.info(s"# broadcastHashUser value head : ${bcHashUser.value.head}")

		// dataset 을 dataframe 로 변환
		val oriUserItemDataDF = rawUserItemData.map { line =>
			// 원천 data 예외처리
			line.split(",") match {
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
		log.info(s"# ${userID} recommDF show!")
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

		log.info(s"# recommDF item name show!")
		// item, item 명
		val itemByRecommID = itemByID
		val itemByRecommIDDF = recommDF.filter($"user" isin (userID)).select('item as "recommItem").join(itemByRecommID, $"id" === $"recommItem", "left_outer").
			select("recommItem", "name")
		itemByRecommIDDF.show(false)

		// hashCode 하기전 user ID 로 다시 변환한다
		val finalRecommDF = filterRecommDF.mapPartitions(rdd => {
			rdd.map(x => {
				val user = (x.getAs[Int]("user"))
				val item = (x.getAs[Int]("item"))
				val prediction = (x.getAs[Float]("prediction"))
				val findOriUser = bcHashUser.value.getOrElse(user, user.toString)
				//https://stackoverflow.com/questions/11106886/scala-doubles-and-precision
				(findOriUser, item, BigDecimal(prediction).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble) // 지수 표기 제외 - 소수점 2자리까지만
				//                (findOriUser, item, "%.2f".format(prediction).toDouble)
			})
		}).toDF("user", "item", "prediction")

		filterRecommDF.unpersist(false)
		log.info(s"# ${oriUserID}(${userID}) finalRecommDF show!")
		finalRecommDF.filter($"user" isin (oriUserID)).show(false)

		// 전일자 조회
		val p_yymmdd = CommonsUtil.getMinus1DaysDate()

		// 최종 추천 결과 hdfs 저장
		log.info(s"# hdfs save start")
		//        finalRecommDF.withColumn("prediction", expr("CAST(prediction AS FLOAT)")).coalesce(1).write
		finalRecommDF.coalesce(1).write
			.mode(SaveMode.Overwrite)
			//            .format("com.databricks.spark.csv")
			.option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/1/p_yymmdd=" + p_yymmdd)
		//            .option("delimiter", "\036").csv("hdfs://daisydp/ml/test/devjackie/picks/result/1/p_yymmdd=" + p_yymmdd)
		//        finalRecommDF.write.mode(SaveMode.Overwrite).option("delimiter", "\u0036").parquet("hdfs://daisydp/ml/test/devjackie/picks/result/2")
		log.info(s"# hdfs save end")

		// broadcast unpersist 는 자동으로 되지만 확실하게 unpersist 해준다
		oriUserItemDataDF.unpersist(true)
		recommDF.unpersist(true)
		model.userFactors.unpersist(true)
		model.itemFactors.unpersist(true)

		// spark context hdfs checkPoint 삭제
		// http://techidiocy.com/java-lang-illegalargumentexception-wrong-fs-expected-file/
		val hadoopConf = spark.sparkContext.hadoopConfiguration
		val fs = FileSystem.get(new URI("hdfs://localhost"), new Configuration(hadoopConf))
		val checkPointPath = new Path("hdfs://localhost" + "/tmp/p_yymmdd=" + p_yymmdd + "/")
		if (fs.exists(checkPointPath)) {
			log.info(fs.getFileStatus(checkPointPath))
			fs.delete(checkPointPath, true)
		}
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

	def evaluate1(trainData: DataFrame, testData: DataFrame): Unit = {

		val inputCols = trainData.columns.filter(_ != "Cover_Type")
		val assembler = new VectorAssembler().
			setInputCols(inputCols).
			setOutputCol("featureVector")

		val classifier = new DecisionTreeClassifier().
			setSeed(Random.nextLong()).
			setLabelCol("Cover_Type").
			setFeaturesCol("featureVector").
			setPredictionCol("prediction")

		val pipeline = new Pipeline().setStages(Array(assembler, classifier))

		val paramGrid = new ParamGridBuilder().
			addGrid(classifier.impurity, Seq("gini", "entropy")).
			addGrid(classifier.maxDepth, Seq(1, 20)).
			addGrid(classifier.maxBins, Seq(40, 300)).
			addGrid(classifier.minInfoGain, Seq(0.0, 0.05)).
			build()

		val multiclassEval = new MulticlassClassificationEvaluator().
			setLabelCol("Cover_Type").
			setPredictionCol("prediction").
			setMetricName("accuracy")

		val validator = new TrainValidationSplit().
			setSeed(Random.nextLong()).
			setEstimator(pipeline).
			setEvaluator(multiclassEval).
			setEstimatorParamMaps(paramGrid).
			setTrainRatio(0.9)

		val validatorModel = validator.fit(trainData)

		val paramsAndMetrics = validatorModel.validationMetrics.
			zip(validatorModel.getEstimatorParamMaps).sortBy(-_._1)

		paramsAndMetrics.foreach { case (metric, params) =>
			println(metric)
			println(params)
			println()
		}

		val bestModel = validatorModel.bestModel

		println(bestModel.asInstanceOf[PipelineModel].stages.last.extractParamMap)

		println(validatorModel.validationMetrics.max)

		val testAccuracy = multiclassEval.evaluate(bestModel.transform(testData))
		println(testAccuracy)

		val trainAccuracy = multiclassEval.evaluate(bestModel.transform(trainData))
		println(trainAccuracy)
	}
}