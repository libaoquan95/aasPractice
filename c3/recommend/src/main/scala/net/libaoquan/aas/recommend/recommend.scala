package net.libaoquan.aas.recommend

import org.apache.spark.SparkContext

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object recommend {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("recommend").master("local").getOrCreate()
    SparkContext.getOrCreate().setLogLevel("WARN")
    import sc.implicits._

    // 加载数据
    val dataDirBase = "D:\\学习\\我的工程\\Scala\\aas\\c3\\profiledata_06-May-2005\\"
    val rawUserArtistData = sc.read.textFile(dataDirBase + "user_artist_data.txt")
    val rawArtistData = sc.read.textFile(dataDirBase + "artist_data.txt")
    val rawArtistAlias = sc.read.textFile(dataDirBase + "artist_alias.txt")

    // 格式化数据
    val artistByID = rawArtistData.flatMap { line =>
      val (id, name) = line.span(_ != '\t')
      if (name.isEmpty()){
        None
      } else {
        try {
          Some((id.toInt, name.trim))
        } catch{
          case _: NumberFormatException => None
        }
      }
    }.toDF("id", "name").cache()

    val artistAlias = rawArtistAlias.flatMap { line =>
      var Array(artist, alias) = line.split('\t')
      if (artist.isEmpty()) {
        None
      } else {
        Some((artist.toInt, alias.toInt))
      }
    }.collect().toMap
    val bArtistAlias = sc.sparkContext.broadcast(artistAlias)

    val userArtistDF = rawUserArtistData.map { line =>
      val Array(userId, artistID, count) = line.split(' ').map(_.toInt)
      val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
      (userId, artistID, count)
    }.toDF("user", "artist", "count").cache()

    // 建立推荐模型
    val Array(trainData, cvData) = userArtistDF.randomSplit(Array(0.9, 0.1))
    val model = new ALS().
      setSeed(Random.nextLong()).
      setImplicitPrefs(true).
      setRank(10).
      setRegParam(0.01).
      setAlpha(1.0).
      setMaxIter(5).
      setUserCol("user").
      setItemCol("artist").
      setRatingCol("count").
      setPredictionCol("prediction").
      fit(trainData)

    // 测试推荐
    val userId = 2093760
    val topN = 10

    val toRecommend = model.itemFactors.
      select($"id".as("artist")).
      withColumn("user", lit(userId))

    val topRecommendations  = model.transform(toRecommend).
      select("artist", "prediction").
      orderBy($"prediction".desc).
      limit(topN)

    // 查看推荐结果
    val recommendedArtistIDs = topRecommendations.select("artist").as[Int].collect()
    artistByID.join(sc.createDataset(recommendedArtistIDs).
      toDF("id"), "id").
      select("name").show()
  }
}
