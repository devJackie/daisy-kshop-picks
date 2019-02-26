package com.kthcorp.daisy.picks

import com.kthcorp.daisy.picks.utils.{BroadcastInstance, HdfsUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{count, _}

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class DevSparkRecommenderExecute_V1(private val spark: SparkSession, private val p_yymmdd: String) extends Serializable {

	@transient lazy val log = Logger.getRootLogger()
	Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
	Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)

	import spark.implicits._

	def preparationMakeData(
		                       rawUserItemData: Dataset[String]): DataFrame = {

		val newRawUserItemDataDF = rawUserItemData.map { lines =>
			lines.split(",") match {
				case Array(user, item, count) => PreUserInfo(user.toString, user.hashCode, item.toInt, count.toInt)
				case Array(user, item, _*) => PreUserInfo(user.toString, user.hashCode, item.toInt, 0)
				case Array(user, _*) => PreUserInfo(user.toString, user.hashCode, 0, 0)
			}
		}.toDF("oriUser", "user", "item", "count")
		newRawUserItemDataDF.createOrReplaceTempView("temp")

		// 구매이력이 4개이상인 user 만 추출
		val userId3monthDF = spark.sql("" +
			"select distinct c.oriUser " +
			"from " +
			"( " +
			"   select " +
			"       b.oriUser, b.count, b.sum_cnt from " +
			"       (" +
			"          select a.oriUser, a.count, a.sum_cnt from " +
			"          (" +
			"              select oriUser, count, count(*) over (partition by oriUser) as sum_cnt " +
			"              from temp " +
			"          ) a " +
			"          group by a.oriUser, a.count, a.sum_cnt " +
			"       ) b where b.sum_cnt >= 5 " +
			") c "
		).toDF("oriUser")

		//        // 구매이력이 10개이하인 user 만 추출
		//        val userId3monthDF = spark.sql("" +
		//            "select distinct c.oriUser " +
		//            "from " +
		//            "( " +
		//            "   select " +
		//            "       b.oriUser, b.count, b.sum_cnt from " +
		//            "       (" +
		//            "          select a.oriUser, a.count, a.sum_cnt from " +
		//            "          (" +
		//            "              select oriUser, count, count(*) over (partition by oriUser) as sum_cnt " +
		//            "              from temp " +
		//            "          ) a " +
		//            "          group by a.oriUser, a.count, a.sum_cnt " +
		//            "       ) b where b.sum_cnt <= 10 " +
		//            ") c "
		//        ).toDF("oriUser")

		//        finalRecommDF.withColumn("prediction", expr("CAST(prediction AS FLOAT)")).coalesce(1).write
		userId3monthDF.coalesce(1).write
			.mode(SaveMode.Overwrite)
			.option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/userId3month/5over/p_yymmdd=" + p_yymmdd)
		//            .option("delimiter", "\036").csv("hdfs://localhost/user/devjackie/picks/result/userId3month/10under/p_yymmdd=" + p_yymmdd)

		//        spark.sql("" +
		//            "select count(distinct c.oriUser) " +
		//            "from " +
		//            "( " +
		//            "   select " +
		//            "       b.oriUser, b.count, b.sum_cnt from " +
		//            "       (" +
		//            "          select a.oriUser, a.count, a.sum_cnt from " +
		//            "          (" +
		//            "              select oriUser, count, count(*) over (partition by oriUser) as sum_cnt " +
		//            "              from temp " +
		//            "          ) a " +
		//            "          group by a.oriUser, a.count, a.sum_cnt " +
		//            "       ) b where b.sum_cnt >= 4 " +
		//            ") c "
		//        ).show(false)
		newRawUserItemDataDF
	}

	def preparationJoinData(
		                       rawUserItemData: Dataset[String]
		                       , rawUserIdData: Dataset[String]): DataFrame = {

		val newRawUserItemDataDF = rawUserItemData.map { lines =>
			lines.split(",") match {
				case Array(user, item, count) => PreUserInfo(user.toString, user.hashCode, item.toInt, count.toInt)
				case Array(user, item, _*) => PreUserInfo(user.toString, user.hashCode, item.toInt, 0)
				case Array(user, _*) => PreUserInfo(user.toString, user.hashCode, 0, 0)
			}
		}.toDF("oriUser", "user", "item", "count")
		newRawUserItemDataDF.createOrReplaceTempView("temp")

		val newRawUserIdDataDF = rawUserIdData.map { lines =>
			lines.split(",") match {
				case Array(user, _*) => (user.toString, user.hashCode)
			}
		}.toDF("oriUser", "user")
		newRawUserIdDataDF.show(10, false)

		// 1-1 user 구매내역정보와 user 추천결과정보를 매핑
		val joinRecommDF = newRawUserItemDataDF.select('oriUser as "oriUserA", 'user as "userA", $"item", $"count")
			.join(newRawUserIdDataDF.select('oriUser as "oriUserB", 'user as "userB")
				, ($"oriUserA" === $"oriUserB")
				, "inner")
		joinRecommDF.select('oriUserA as "oriUser", 'userA as "user", $"item", $"count").show(10, false)
		log.info(joinRecommDF.count())

		// distinct user count (uv)
		joinRecommDF.
			select("userA").as("cnt").agg(countDistinct("userA").as("uv")).show(false)

		//(pv)
		joinRecommDF.createOrReplaceTempView("temp")
		spark.sql("" +
			"       select count(userA) as pv " +
			"       from temp "
		).show(false)

		joinRecommDF.select('oriUserA as "oriUser", 'userA as "user", $"item", $"count").toDF("oriUser", "user", "item", "count")
	}

	def preparationData(
		                   rawUserItemData: Dataset[String]
		                   , rawItemData: Dataset[String]): DataFrame = {

		val newRawUserItemDataDF = rawUserItemData.map { lines =>
			lines.split(",") match {
				case Array(user, item, count) => PreUserInfo(user.toString, user.hashCode, item.toInt, count.toInt)
				case Array(user, item, _*) => PreUserInfo(user.toString, user.hashCode, item.toInt, 0)
				case Array(user, _*) => PreUserInfo(user.toString, user.hashCode, 0, 0)
			}
		}.toDF("oriUser", "user", "item", "count")
		newRawUserItemDataDF.createOrReplaceTempView("temp")
		// 구매이력 원하는 개수대로 추출
		//        spark.sql("" +
		//            "select " +
		//            "distinct b.oriUser, b.count, b.sum_cnt from " +
		//            "(" +
		//            "   select a.oriUser, a.count, a.sum_cnt from " +
		//            "   (" +
		//            "       select oriUser, count, count(*) over (partition by oriUser) as sum_cnt " +
		//            "       from temp " +
		//            "   ) a " +
		//            "   group by a.oriUser, a.count, a.sum_cnt " +
		//            ") b where b.sum_cnt >= 2 "
		//        ).show(false)

		// n건이상 구매한 사용자
		spark.sql("" +
			"select count(*) as cnt from " +
			"( " +
			"select " +
			"b.oriUser, b.sum_cnt from " +
			"(" +
			"   select a.oriUser, a.count, a.sum_cnt from " +
			"   (" +
			"       select oriUser, count, count(*) over (partition by oriUser) as sum_cnt " +
			"       from temp " +
			"   ) a " +
			"   group by a.oriUser, a.count, a.sum_cnt " +
			") b where b.sum_cnt >= 2 " +
			"group by b.oriUser, b.sum_cnt" +
			") c"
		).show(false)

		// uv 사용자 추출
		//        spark.sql("" +
		//            "       select count(distinct user) as uv " +
		//            "       from temp "
		//        ).show(false)
		val userID = -738077331
		val oriUserID = "201603470983"
		val itemByID = buildItemByID(rawItemData)
		log.info(s">> user(${userID}) 구매 정보 => item id, name")
		val existingItemIDs = newRawUserItemDataDF.
			filter($"user" === userID).
			select("item").as[Int].collect()
		// item, item 명
		val itemByRecommID = itemByID
		val itemByRecommIDDF = newRawUserItemDataDF.filter($"user" isin (userID)).select('item as "recommItem").join(itemByRecommID, $"id" === $"recommItem", "left_outer").
			select("recommItem", "name")
		itemByRecommIDDF.show(false)
		spark.sql(s"select oriUser, item, count from temp where oriUser = ${oriUserID}  ").show(false)
		newRawUserItemDataDF
	}

	def preparation(
		               rawUserItemData: Dataset[String]): DataFrame = {

		val newRawUserItemDataDF = rawUserItemData.map { lines =>
			lines.split(",") match {
				case Array(user, item, count) => PreUserInfo(user.toString, user.hashCode, item.toInt, count.toInt)
				case Array(user, item, _*) => PreUserInfo(user.toString, user.hashCode, item.toInt, 0)
				case Array(user, _*) => PreUserInfo(user.toString, user.hashCode, 0, 0)
			}
		}.toDF("oriUser", "user", "item", "count")
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

	def evaluateAUCrow(newRawUserItemData: DataFrame): Unit = {

		val allData = newRawUserItemData.cache()

		val Array(trainData, cvData) = allData.randomSplit(Array(0.9, 0.1))
		trainData.cache()
		cvData.cache()

		val allArtistIDs = allData.select("item").as[Int].distinct().collect()
		val bAllArtistIDs = spark.sparkContext.broadcast(allArtistIDs)

		val mostListenedAUC = areaUnderCurve(cvData, bAllArtistIDs, predictMostListened(trainData))
		println(mostListenedAUC)

		val evaluations =
		//            for (rank     <- Seq(10,  30);
		//                 regParam <- Seq(1.0, 0.1);
		//                 alpha    <- Seq(1.0, 40.0))
		//            for (rank     <- Seq(10,  30);
		//                 regParam <- Seq(2.0, 10.0);
		//                 alpha    <- Seq(70.0, 50.0))
		//            for (rank     <- Seq(1, 5, 10, 30);
		//                 regParam <- Seq(0.0001, 0.001, 0.01, 1.0, 4.0);
		//                 alpha    <- Seq(1.0, 25.0, 40.0))
		//            for (rank     <- Seq(2, 20);
		//                 regParam <- Seq(0.01, 0.1);
		//                 alpha    <- Seq(1.0, 10.0))
		//            for (rank     <- Seq(5,  10);
		//                 regParam <- Seq(4.0, 0.01);
		//                 alpha    <- Seq(1.0, 10.0))
			for (rank <- Seq(10);
			     regParam <- Seq(1.0);
			     alpha <- Seq(40.0))
				yield {
					val model = new ALS().
						setSeed(Random.nextLong()).
						setImplicitPrefs(true).
						setRank(rank).setRegParam(regParam).
						setAlpha(alpha).setMaxIter(20).
						setUserCol("user").setItemCol("item").
						setRatingCol("count").setPredictionCol("prediction").
						fit(trainData)

					val auc = areaUnderCurveRow(cvData, bAllArtistIDs, model.transform)

					model.userFactors.unpersist()
					model.itemFactors.unpersist()

					auc.foreach(a => println(s"${a}"))
					auc.map(elem => (elem, (rank, regParam, alpha)))
					//                    (auc, (rank, regParam, alpha))
				}

		evaluations.reverse.foreach(println)

		trainData.unpersist()
		cvData.unpersist()
	}

	def evaluate(newRawUserItemData: DataFrame): Unit = {

		val allData = newRawUserItemData.cache()

		val Array(trainData, cvData) = allData.randomSplit(Array(0.9, 0.1))
		trainData.cache()
		cvData.cache()

		val allArtistIDs = allData.select("item").as[Int].distinct().collect()
		val bAllArtistIDs = spark.sparkContext.broadcast(allArtistIDs)

		val mostListenedAUC = areaUnderCurve(cvData, bAllArtistIDs, predictMostListened(trainData))
		println(mostListenedAUC)

		val evaluations =
			for (rank <- Seq(10, 28);
			     regParam <- Seq(3.0, 1.0, 0.1);
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

	//    def modelSave(
	//                     rawUserItemData: Dataset[String],
	//                     rawItemData: Dataset[String],
	//                     allData: DataFrame): Unit = {
	//
	//        log.info(s"# recommend start")
	//
	//        allData.cache()
	//
	//        // model의 추첨 점수가 1 이상인 경우에 대한 설명
	//        // https://stackoverflow.com/questions/46904078/spark-als-recommendation-system-have-value-prediction-greater-than-1
	//        val model = new ALS()
	//            .setSeed(Random.nextLong())
	//            .setImplicitPrefs(true)
	//            .setRank(28).setRegParam(1.0).setAlpha(40.0).setMaxIter(20)
	//            .setUserCol("user").setItemCol("item")
	//            .setRatingCol("count").setPredictionCol("prediction")
	//            .setCheckpointInterval(2)
	//            .setColdStartStrategy("drop") // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics (NaN 제거)
	//            .setNonnegative(true) // Apache Spark provides an option to force non negative constraints on ALS. (음수제거)
	//            .fit(allData)
	//
	//        allData.unpersist(true)
	//
	//        model.save("hdfs://localhost/tmp/model/p_yymmdd=" + p_yymmdd)
	//
	////        // broadcast unpersist 는 자동으로 되지만 확실하게 unpersist 해준다
	////        model.userFactors.unpersist(true)
	////        model.itemFactors.unpersist(true)
	//    }
	//
	//    def recommend(
	//                     rawUserItemData: Dataset[String],
	//                     rawItemData: Dataset[String],
	//                     allData: DataFrame): Unit = {
	//
	//        log.info(s"# recommend start")
	//
	//        allData.cache()
	//
	//
	//        val loadModel = ALSModel.load("hdfs://localhost/tmp/model/p_yymmdd=" + p_yymmdd)
	//
	////        val newModel = new ALS()
	////            .setUserCol("user").setItemCol("item")
	////            .setRatingCol("count").setPredictionCol("prediction")
	////            .fit(allData)
	//        val modelStages = ArrayBuffer.empty[PipelineStage]
	//        modelStages += loadModel
	////        modelStages += newModel
	//        val pipeline = new Pipeline().setStages(modelStages.toArray).fit(allData)
	//
	////        val pipelineModel = pipeline.fit(allData)
	////        val pipelines = pipeline.setStages(pipelineModel.stages)
	//        val model = pipeline.stages(0).asInstanceOf[ALSModel]
	//
	//
	//
	//        allData.unpersist(true)
	//
	////        val userID = 145044192
	////        val userID = -777771416 // prediction 에 지수로 표시되는 데이터가 존재
	////        val userID = 81099459 // 지수가 많음
	////        val userID = 940930974 // 지수가 많음
	////        val userID = 890823203 // 지수가 많음
	////        val oriUserID = "201702579345" // 지수가 많음
	////        val userID = -1775785176 // 추천 예상 평가 점수가 높음 (구매이력 4개)
	////        val oriUserID = "201610037428" // 추천 예상 평가 점수가 높음 (구매이력 4개)
	////        val userID = -23546283 // 예상 평가 점수 0.5~0.6 (구매이력 3개, 구매이력중 value 가 2이상인게 2개)
	////        val oriUserID = "201701496919" // 예상 평가 점수 0.5~0.6 (구매이력 3개, 구매이력중 value 가 2이상인게 2개)
	////        val userID = -1365386275 // (구매이력 15개)
	////        val oriUserID = "201505768395" //  (구매이력 15개)
	////        val userID = -1545748625 // 구매건수 value 이 너무 많음, 업자같음.. 데이터를 제외시켜야 할듯
	////        val oriUserID = "201709357546" // 구매건수 value 이 너무 많음, 업자같음.. 데이터를 제외시켜야 할듯
	////        val userID = 1980943308 // 구매이력 1건이고 value 값이 28개
	////        val oriUserID = "201606666559" // 구매이력 1건이고 value 값이 28개
	////        val userID = -778879351 // 구매이력과 value 값이 42개 많은 유저
	////        val oriUserID = "201807523058" // 구매이력과 value 값이 42개 많은 유저
	////        val userID = -1363599091 // 구매이력 10건
	////        val oriUserID = "201505786308" // 구매이력 10건
	////        val userID = 439945189 // 구매이력 5
	////        val oriUserID = "201507883010" // 구매이력 5
	////        val userID = -626284095 // 구매이력 6
	////        val oriUserID = "201705986186" // 구매이력 6
	////        val userID = -1576520766 // 구매이력 8
	////        val oriUserID = "201712864124" // 구매이력 8
	////        val userID = -625503762 // 구매이력 8
	////        val oriUserID = "201705991787" // 구매이력 8
	////        val userID = 462058977 // 구매이력 10
	////        val oriUserID = "201507911980" // 구매이력 10
	////        val userID = -838070106 // 구매이력 30
	////        val oriUserID = "201807300492" // 구매이력 30
	////        val userID = -737008662 // 구매이력 20
	////        val oriUserID = "201603485599" // 구매이력 20
	////        val userID = 475686613 // 구매이력 1
	////        val oriUserID = "201206141718" // 구매이력 1
	//        val userID = -209359097 // 구매이력 1
	//        val oriUserID = "201811020000" // 구매이력 1
	//
	//
	////        val userID = -1775261572 // 예상 평가 점수 0.4~0.6 (구매이력 1개)
	////        val oriUserID = "201806186819" // 예상 평가 점수 0.4~0.6 (구매이력 1개)
	////        val userID = -1774947186 // 예상 평가 점수 0.6~0.9 (구매이력 5개)
	////        val oriUserID = "201610044826" // 예상 평가 점수 0.6~0.9 (구매이력 5개)
	////        val userID = -1694473283 // 예상 평가 점수 0.4~0.5 (구매이력 2개)
	////        val oriUserID = "201801921450" // 예상 평가 점수 0.4~0.5 (구매이력 2개)
	////        val userID = 1062038887 // 예상 평가 점수 0.5~0.7 (구매이력 3개, 남성, 여성 제품 구매)
	////        val oriUserID = "201605536436" // 예상 평가 점수 0.5~0.7 (구매이력 3개, 남성, 여성 제품 구매)
	////        val userID = 428891843 // 예상 평가 점수 0.4~0.6 (구매이력 4개, 구매이력중 value 가 전부 1)
	////        val oriUserID = "201609908404" // 예상 평가 점수 0.4~0.6 (구매이력 4개, 구매이력중 value 가 전부 1)
	////        val userID = -2050339956 // 예상 평가 점수 1이상 (구매이력 16개, 구매이력중 value 가 2이상인게 1개)
	////        val oriUserID = "201509997338" // 예상 평가 점수 1이상 (구매이력 16개, 구매이력중 value 가 2이상인게 1개)
	////        val userID = -1654122711 // 예상 평가 점수 0.7~0.9 (구매이력 3개, 구매이력중 value 가 2이상인게 2개)
	////        val oriUserID = "201602373784" // 예상 평가 점수 0.7~0.9 (구매이력 3개, 구매이력중 value 가 2이상인게 2개)
	////        val userID = 431428174 // 예상 평가 점수 0.7~0.8 (구매이력 3개, 구매이력중 value 가 전부 1)
	////        val oriUserID = "201609930888" // 예상 평가 점수 0.7~0.8 (구매이력 3개, 구매이력중 value 가 전부 1)
	////        val userID = 1803082341 // 예상 평가 점수 0.7~0.8 (구매이력 3개, 구매이력중 value 가 전부 1)
	////        val oriUserID = "201703633279" // 예상 평가 점수 0.7~0.8 (구매이력 3개, 구매이력중 value 가 전부 1)
	////        val userID = -1544880847 //
	////        val oriUserID = "201709365941" //
	////        val userID = -738077331
	////        val oriUserID = "201603470983"
	//
	//        allData.createOrReplaceTempView("temp1")
	//        spark.sql(s"select oriUser, item, count from temp1 where oriUser = '${oriUserID}'  ").show(false)
	//
	//        val existingItemIDs = allData.
	//            filter($"user" === userID).
	//            select("item").as[Int].collect()
	//
	//        val itemByID = buildItemByID(rawItemData)
	//        log.info(s">> user(${userID}) 구매 정보 => item id, name")
	//        itemByID.filter($"id" isin (existingItemIDs:_*)).show(1000,false)
	//
	//        log.info(s">> user(${userID}) recommendations")
	//        val recommData = model.recommendForAllUsers(10)
	////        val recommData = model.recommendForUserSubset(allData, 10)
	////        recommData.filter($"id" isin (userID)).show(false)
	//
	//        // user, hashUser Map 데이터 생성
	//        val bcHashUser: Broadcast[Map[Int, String]] = BroadcastInstance.getBroadCastHashUserData(spark.sparkContext, spark, buildHashUserMap(rawUserItemData))
	//        log.info(s"# broadcastHashUser value size : ${bcHashUser.value.size}")
	//        log.info(s"# broadcastHashUser value head : ${bcHashUser.value.head}")
	//
	//        // dataset 을 dataframe 로 변환
	//        val oriUserItemDataDF = rawUserItemData.map { line =>
	//            // 원천 data 예외처리
	//            line.split(",") match {
	//                case Array(user, item, count) => OriUserInfo(user.hashCode, item.toInt, count.toInt)
	//                case Array(user, item, _*) => OriUserInfo(user.hashCode, item.toInt, 0)
	//                case Array(user, _*) => OriUserInfo(user.hashCode, 0, 0)
	//            }
	//        }.toDF("user", "item", "count")
	////            .cache()
	//        oriUserItemDataDF.cache()
	//        log.info(s"# oriUserItemDataDF show!")
	////        oriUserItemDataDF.show(10, false)
	////        oriUserItemDataDF.filter($"user" isin (userID)).show(50, false)
	//
	//        // mapPartition
	//        // 기존 dataframe 의 user, recommendations -> 신규 dataframe 의 user, item, prediction 으로 변환
	//        //https://stackoverflow.com/questions/42354372/spark-explode-a-dataframe-array-of-structs-and-append-id
	//        //https://hadoopist.wordpress.com/2016/05/16/how-to-handle-nested-dataarray-of-structures-or-multiple-explodes-in-sparkscala-and-pyspark/
	//        val recommDF = recommData.select($"user", explode($"recommendations")).select($"user", $"col.item", $"col.rating" as "prediction").toDF()
	////            .cache()
	//        log.info(s"# recommDF show!")
	//        recommDF.cache()
	//        log.info(s"# ${userID} recommDF show!")
	////        recommDF.filter($"user" isin (userID)).show(false)
	//
	////##############################################################################
	////########################### 기존 추천 제외 필터링 #################################
	////##############################################################################
	//        // 1-1 user 구매내역정보와 user 추천결과정보를 매핑
	//        val joinRecommDF = recommDF.select('user as "user", 'item as "item", 'prediction as "prediction")
	//            .join(oriUserItemDataDF.select('user as "oriUser", 'item as "oriItem", 'count as "oriCount")
	//                , ($"user" === $"oriUser") && ($"item" === $"oriItem")
	//                , "inner")
	//        log.info(s"# joinRecommDF show!")
	//        //        joinRecommDF.show(10, false)
	////        joinRecommDF.filter($"user" isin (userID)).show(50, false)
	//
	//
	//        log.info(s"# recommDF2 show!")
	//        //        filterRecommDF.show(10,false)
	////        recommDF.filter($"user" isin (userID)).show(50,false)
	//        // 1-2 user 추천결과정보에서 1-1 의 매핑 정보를 제외
	//        // user 마다 기존에 구매했던 상품 제외
	//        val filterRecommDF = recommDF.select('user as "user", 'item as "item", 'prediction as "prediction")
	//            .except(joinRecommDF.select('user as "user", 'item as "item", 'prediction as "prediction"))
	////        log.info(s"# recommDF3 show!")
	////        //        filterRecommDF.show(10,false)
	////        recommDF.filter($"user" isin (userID)).show(50,false)
	//        log.info(s"# filterRecommDF show!")
	////        filterRecommDF.filter($"user" isin (userID)).show(50,false)
	////##############################################################################
	//
	//        log.info(s"# recommDF item name show!")
	//        // item, item 명
	//        val itemByRecommID = itemByID
	//        val itemByRecommIDDF = recommDF.filter($"user" isin (userID)).select('item as "recommItem").join(itemByRecommID, $"id" === $"recommItem", "left_outer").
	//            select("recommItem", "name")
	//        itemByRecommIDDF.orderBy(asc("recommItem")).show(false)
	//
	//        // hashCode 하기전 user ID 로 다시 변환한다
	//        val finalRecommDF = recommDF.mapPartitions( rdd => {
	//            rdd.map(x => {
	//                val user = (x.getAs[Int]("user"))
	//                val item = (x.getAs[Int]("item"))
	//                val prediction = (x.getAs[Float]("prediction"))
	//                val findOriUser = bcHashUser.value.getOrElse(user, user.toString)
	//                //https://stackoverflow.com/questions/11106886/scala-doubles-and-precision
	////                (findOriUser, item, prediction)
	//                (findOriUser, item, BigDecimal(prediction).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble) // 지수 표기 제외 - 소수점 2자리까지만
	////                (findOriUser, item, "%.2f".format(prediction).toDouble)
	//            })
	//        }).toDF("user", "item", "prediction")
	//
	//        filterRecommDF.unpersist(false)
	//        log.info(s"# ${oriUserID}(${userID}) finalRecommDF show!")
	//        finalRecommDF.filter($"user" isin (oriUserID)).orderBy(asc("item")).show(false)
	//
	//        // 최종 추천 결과 hdfs 저장
	//        log.info(s"# hdfs save start")
	////        HdfsUtil.devSaveAsHdfsForRecomm(finalRecommDF, p_yymmdd)
	//        log.info(s"# hdfs save end")
	//
	//        // broadcast unpersist 는 자동으로 되지만 확실하게 unpersist 해준다
	//        oriUserItemDataDF.unpersist(true)
	//        recommDF.unpersist(true)
	//        model.userFactors.unpersist(true)
	//        model.itemFactors.unpersist(true)
	//    }

	def recommend(
		             rawUserItemData: Dataset[String],
		             rawItemData: Dataset[String],
		             allData: DataFrame): Unit = {

		log.info(s"# recommend start")

		allData.cache()

		// model의 추첨 점수가 1 이상인 경우에 대한 설명
		// https://stackoverflow.com/questions/46904078/spark-als-recommendation-system-have-value-prediction-greater-than-1
		val model = new ALS()
			.setSeed(Random.nextLong())
			.setImplicitPrefs(true)
			//            .setRank(20).setRegParam(1.0).setAlpha(40.0).setMaxIter(20) // default
			//            .setRank(10).setRegParam(1.0).setAlpha(40.0).setMaxIter(100) // tuning
			//            .setRank(10).setRegParam(1.0).setAlpha(1.0).setMaxIter(20) // 2.0넘는 점수없고, 아이템별 카운트가 많은 순으로 점수가 잘나옴
			//            .setRank(10).setRegParam(1.0).setAlpha(1.0).setMaxIter(20) // tuning
			//            .setRank(10).setRegParam(1.0).setAlpha(1.0).setMaxIter(20) // tuning 구매이력 15개(201505768395) 는 구매이력이 있는 상품을 추천했지만 추천수치가 낮음..
			//            .setRank(10).setRegParam(6.0).setAlpha(20.0).setMaxIter(20) // 추천 점수 평이하게 나온 값, (구매이력 많고,value 많고), (구매이력 4건, value값 적고), (구매이력 1건, value값 많고) 하지만 추천 결과가 이상함
			//            .setRank(20).setRegParam(1.0).setAlpha(1.0).setMaxIter(20) // (구매이력 많고,value 많고) 추천결과
			//            .setRank(20).setRegParam(1.5).setAlpha(1.0).setMaxIter(20) // (구매이력 많고,value 많고) 추천결과

			//구매이력 많고, value 적을때
			.setRank(28).setRegParam(1.0).setAlpha(40.0).setMaxIter(20)
			//            .setRank(10).setRegParam(1.0).setAlpha(1.0).setMaxIter(20)
			//구매이력 많고, value 많을 때
			//            .setRank(20).setRegParam(1.5).setAlpha(2.0).setMaxIter(20) // 괜찮음

			//            .setRank(20).setRegParam(0.0001).setAlpha(40.0).setMaxIter(100) //1.011031866963691
			//            .setRank(20).setRegParam(0.1).setAlpha(40.0).setMaxIter(20) //1.0045638398846983
			//            .setRank(20).setRegParam(1.0).setAlpha(40.0).setMaxIter(20) //0.9992248180603216
			//            .setRank(30).setRegParam(1.0).setAlpha(1.0).setMaxIter(100)
			//            .setRank(1).setRegParam(0.0001).setAlpha(25.0).setMaxIter(100)
			//            .setRank(10).setRegParam(0.1).setAlpha(40.0).setMaxIter(100)
			//            .setRank(10).setRegParam(1.0).setAlpha(10.0).setMaxIter(100)
			.setUserCol("user").setItemCol("item")
			.setRatingCol("count").setPredictionCol("prediction")
			.setCheckpointInterval(2)
			.setColdStartStrategy("drop") // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics (NaN 제거)
			.setNonnegative(true) // Apache Spark provides an option to force non negative constraints on ALS. (음수제거)
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
		//        val userID = -23546283 // 예상 평가 점수 0.5~0.6 (구매이력 3개, 구매이력중 value 가 2이상인게 2개)
		//        val oriUserID = "201701496919" // 예상 평가 점수 0.5~0.6 (구매이력 3개, 구매이력중 value 가 2이상인게 2개)
		val userID = -1365386275 // (구매이력 15개)
		val oriUserID = "201505768395" //  (구매이력 15개)
		//                val userID = -1545748625 // 구매건수 value 이 너무 많음, 업자같음.. 데이터를 제외시켜야 할듯
		//                val oriUserID = "201709357546" // 구매건수 value 이 너무 많음, 업자같음.. 데이터를 제외시켜야 할듯
		//        val userID = 1980943308 // 구매이력 1건이고 value 값이 28개
		//        val oriUserID = "201606666559" // 구매이력 1건이고 value 값이 28개
		//                val userID = -778879351 // 구매이력과 value 값이 42개 많은 유저
		//                val oriUserID = "201807523058" // 구매이력과 value 값이 42개 많은 유저
		//        val userID = -1363599091 // 구매이력 10건
		//        val oriUserID = "201505786308" // 구매이력 10건
		//        val userID = 439945189 // 구매이력 5
		//        val oriUserID = "201507883010" // 구매이력 5
		//        val userID = -626284095 // 구매이력 6
		//        val oriUserID = "201705986186" // 구매이력 6
		//        val userID = -1576520766 // 구매이력 8
		//        val oriUserID = "201712864124" // 구매이력 8
		//        val userID = -625503762 // 구매이력 8
		//        val oriUserID = "201705991787" // 구매이력 8
		//                val userID = 462058977 // 구매이력 10
		//                val oriUserID = "201507911980" // 구매이력 10
		//        val userID = -838070106 // 구매이력 30
		//        val oriUserID = "201807300492" // 구매이력 30
		//        val userID = -737008662 // 구매이력 20
		//        val oriUserID = "201603485599" // 구매이력 20
		//        val userID = 475686613 // 구매이력 1
		//        val oriUserID = "201206141718" // 구매이력 1


		//        val userID = -1775261572 // 예상 평가 점수 0.4~0.6 (구매이력 1개)
		//        val oriUserID = "201806186819" // 예상 평가 점수 0.4~0.6 (구매이력 1개)
		//        val userID = -1774947186 // 예상 평가 점수 0.6~0.9 (구매이력 5개)
		//        val oriUserID = "201610044826" // 예상 평가 점수 0.6~0.9 (구매이력 5개)
		//        val userID = -1694473283 // 예상 평가 점수 0.4~0.5 (구매이력 2개)
		//        val oriUserID = "201801921450" // 예상 평가 점수 0.4~0.5 (구매이력 2개)
		//        val userID = 1062038887 // 예상 평가 점수 0.5~0.7 (구매이력 3개, 남성, 여성 제품 구매)
		//        val oriUserID = "201605536436" // 예상 평가 점수 0.5~0.7 (구매이력 3개, 남성, 여성 제품 구매)
		//        val userID = 428891843 // 예상 평가 점수 0.4~0.6 (구매이력 4개, 구매이력중 value 가 전부 1)
		//        val oriUserID = "201609908404" // 예상 평가 점수 0.4~0.6 (구매이력 4개, 구매이력중 value 가 전부 1)
		//        val userID = -2050339956 // 예상 평가 점수 1이상 (구매이력 16개, 구매이력중 value 가 2이상인게 1개)
		//        val oriUserID = "201509997338" // 예상 평가 점수 1이상 (구매이력 16개, 구매이력중 value 가 2이상인게 1개)
		//        val userID = -1654122711 // 예상 평가 점수 0.7~0.9 (구매이력 3개, 구매이력중 value 가 2이상인게 2개)
		//        val oriUserID = "201602373784" // 예상 평가 점수 0.7~0.9 (구매이력 3개, 구매이력중 value 가 2이상인게 2개)
		//        val userID = 431428174 // 예상 평가 점수 0.7~0.8 (구매이력 3개, 구매이력중 value 가 전부 1)
		//        val oriUserID = "201609930888" // 예상 평가 점수 0.7~0.8 (구매이력 3개, 구매이력중 value 가 전부 1)
		//        val userID = 1803082341 // 예상 평가 점수 0.7~0.8 (구매이력 3개, 구매이력중 value 가 전부 1)
		//        val oriUserID = "201703633279" // 예상 평가 점수 0.7~0.8 (구매이력 3개, 구매이력중 value 가 전부 1)
		//        val userID = -1544880847 //
		//        val oriUserID = "201709365941" //
		//        val userID = -738077331
		//        val oriUserID = "201603470983"

		allData.createOrReplaceTempView("temp1")
		spark.sql(s"select oriUser, item, count from temp1 where oriUser = '${oriUserID}'  ").show(100, false)

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

		//##############################################################################
		//########################### 기존 추천 제외 필터링 #################################
		//##############################################################################
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
		//##############################################################################

		log.info(s"# recommDF item name show!")
		// item, item 명
		val itemByRecommID = itemByID
		val itemByRecommIDDF = recommDF.filter($"user" isin (userID)).select('item as "recommItem").join(itemByRecommID, $"id" === $"recommItem", "left_outer").
			select("recommItem", "name")
		itemByRecommIDDF.orderBy(asc("recommItem")).show(false)

		// hashCode 하기전 user ID 로 다시 변환한다
		val finalRecommDF = recommDF.mapPartitions(rdd => {
			rdd.map(x => {
				val user = (x.getAs[Int]("user"))
				val item = (x.getAs[Int]("item"))
				val prediction = (x.getAs[Float]("prediction"))
				val findOriUser = bcHashUser.value.getOrElse(user, user.toString)
				//https://stackoverflow.com/questions/11106886/scala-doubles-and-precision
				//                (findOriUser, item, prediction)
				(findOriUser, item, BigDecimal(prediction).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble) // 지수 표기 제외 - 소수점 2자리까지만
				//                (findOriUser, item, "%.2f".format(prediction).toDouble)
			})
		}).toDF("user", "item", "prediction")

		filterRecommDF.unpersist(false)
		log.info(s"# ${oriUserID}(${userID}) finalRecommDF show!")
		finalRecommDF.filter($"user" isin (oriUserID)).orderBy(asc("item")).show(false)

		// 최종 추천 결과 hdfs 저장
		log.info(s"# hdfs save start")
		HdfsUtil.devSaveAsHdfsForRecomm(finalRecommDF, p_yymmdd)
		log.info(s"# hdfs save end")

		// broadcast unpersist 는 자동으로 되지만 확실하게 unpersist 해준다
		oriUserItemDataDF.unpersist(true)
		recommDF.unpersist(true)
		model.userFactors.unpersist(true)
		model.itemFactors.unpersist(true)
	}

	def areaUnderCurveRow(
		                     positiveData: DataFrame,
		                     bAllArtistIDs: Broadcast[Array[Int]],
		                     predictFunction: (DataFrame => DataFrame)): Array[Row] = {

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
			.cache()

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
			select((coalesce($"correct", lit(0)) / $"total").as("auc")).
			collect()

		joinedPredictions.unpersist()

		meanAUC
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