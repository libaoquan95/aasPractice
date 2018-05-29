package net.libaoquan.aas.rdf

import org.apache.spark.SparkContext
import org.apache.spark.ml.{PipelineModel, Pipeline}
import org.apache.spark.ml.classification.{DecisionTreeClassifier,
  RandomForestClassifier, RandomForestClassificationModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import scala.util.Random

object rdf {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("rdf").master("local").getOrCreate()
    SparkContext.getOrCreate().setLogLevel("WARN")
    import sc.implicits._

    // 加载数据
    //val dataDir = "D:\\学习\\我的工程\\Scala\\aas\\c4\\covtype\\covtype.data"
    val dataDir = "covtype.data"
    val dataWithoutHeader = sc.read. option("inferSchema", true).option("header", false). csv(dataDir)

    // 结构化数据
    val colNames = Seq(
      "Elevation", "Aspect", "Slope",
      "Horizontal_Distance_To_Hydrology", "Vertical_Distance_To_Hydrology",
      "Horizontal_Distance_To_Roadways",
      "Hillshade_9am", "Hillshade_Noon", "Hillshade_3pm",
      "Horizontal_Distance_To_Fire_Points"
    ) ++ (
      (0 until 4).map(i => s"Wilderness_Area_$i")
      ) ++ (
      (0 until 40).map(i => s"Soil_Type_$i")
      ) ++ Seq("Cover_Type")
    val data = dataWithoutHeader.toDF(colNames:_*).
      withColumn("Cover_Type", $"Cover_Type".cast("double"))
    val Array(trainData, testData) = data.randomSplit(Array(0.9, 0.1))
    trainData.cache()
    testData.cache()

    // 单颗决策树
    // 构造特征向量
    val inputCols = trainData.columns.filter(_ != "Cover_Type")
    val assembler = new VectorAssembler().setInputCols(inputCols).setOutputCol("featureVector")
    val assembledTrainData = assembler.transform(trainData)

    val classifier = new DecisionTreeClassifier().
      setSeed(Random.nextLong()).
      setLabelCol("Cover_Type").
      setFeaturesCol("featureVector").
      setPredictionCol("prediction")

    val model = classifier.fit(assembledTrainData)
  }
}
