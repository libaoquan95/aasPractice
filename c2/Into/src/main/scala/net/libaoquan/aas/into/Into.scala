package net.libaoquan.aas.into

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

case class MatchData (
   id_1: Int,
   id_2: Int,
   cmp_fname_c1: Option[Double],
   cmp_fname_c2: Option[Double],
   cmp_lname_c1: Option[Double],
   cmp_lname_c2: Option[Double],
   cmp_sex: Option[Int],
   cmp_bd: Option[Int],
   cmp_bm: Option[Int],
   cmp_by: Option[Int],
   cmp_plz: Option[Int],
   is_match: Boolean
 )

object Into extends Serializable {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("Into").master("local").getOrCreate()
    import sc.implicits._

    val dataDir = "D:\\学习\\我的工程\\Scala\\aas\\c2\\linkage\\block_*.csv"
    //val preview = sc.read.csv(dataDir)
    //preview.show()
    //preview.printSchema()
    val parsed = sc.read .option("header", "true") .option("nullValue", "?") .option("inferSchema", "true") .csv(dataDir)
    //parsed.show()  //查看表
    //parsed.printSchema() //查看表结构
    parsed.cache()

    // 聚合
    parsed.groupBy("is_match").count().orderBy($"count".desc).show()

    // 先注册为临时表
    parsed.createOrReplaceTempView("linkage")
    // 使用sql查询，效果同上
    sc.sql("""
      SELECT is_match, COUNT(*) cnt
      FROM linkage
      GROUP BY is_match
      ORDER BY cnt DESC
    """).show()

    // 获取每一列的最值，平均值信息
    val summary = parsed.describe()
    //summary.show()
    summary.select("summary", "cmp_fname_c1", "cmp_fname_c2").show()

    // 获取匹配和不匹配的信息
    val matches = parsed.where("is_match = true")
    val misses = parsed.filter($"is_match" === false)
    val matchSummary = matches.describe()
    val missSummary = misses.describe()

    // 转置，重塑数据
    val matchSummaryT = pivotSummary(matchSummary)
    val missSummaryT = pivotSummary(missSummary)
    matchSummaryT.createOrReplaceTempView("match_desc")
    missSummaryT.createOrReplaceTempView("miss_desc")
    sc.sql("""
      SELECT a.field, a.count + b.count total, a.mean - b.mean delta
      FROM match_desc a INNER JOIN miss_desc b ON a.field = b.field
      ORDER BY delta DESC, total DESC
    """).show()

    // 结构化数据
    val matchData = parsed.as[MatchData]
    val scored = matchData.map { md =>
      (scoreMatchData(md), md.is_match)
    }.toDF("score", "is_match")
    crossTabs(scored, 4.0).show()
  }

  def crossTabs(scored: DataFrame, t: Double): DataFrame = {
    scored.
      selectExpr(s"score >= $t as above", "is_match").
      groupBy("above").
      pivot("is_match", Seq("true", "false")).
      count()
  }

  case class Score(value: Double) {
    def +(oi: Option[Int]) = {
      Score(value + oi.getOrElse(0))
    }
  }

  def scoreMatchData(md: MatchData): Double = {
    (Score(md.cmp_lname_c1.getOrElse(0.0)) + md.cmp_plz +
      md.cmp_by + md.cmp_bd + md.cmp_bm).value
  }

  def longForm(desc: DataFrame): DataFrame = {
    import desc.sparkSession.implicits._ // For toDF RDD -> DataFrame conversion
    val schema = desc.schema
    desc.flatMap(row => {
      val metric = row.getString(0)
      (1 until row.size).map(i => (metric, schema(i).name, row.getString(i).toDouble))
    })
      .toDF("metric", "field", "value")
  }
  def pivotSummary(desc: DataFrame): DataFrame = {
    val lf = longForm(desc)
    lf.groupBy("field").
      pivot("metric", Seq("count", "mean", "stddev", "min", "max")).
      agg(first("value"))
  }
}
