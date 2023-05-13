package com.cqyt.paper.ml

import com.cqyt.paper.config.SparkInitiation.spark
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{GBTClassifier, LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{ChiSqSelector, PCA, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object DataMining {

  val featurePath = "file:///opt/feature"

  def main(args: Array[String]): Unit = {

    val df = spark.read.orc("F:\\featureCombine")
    val sample0 = df.filter(col("label")===1)
    val sample1 = df.filter(col("label")===0).sample(0.07)
    val train = sample0.union(sample1)

    val features: Array[String] = Array("u1_latest_click_time", "u1_latest_collect_time", "u1_latest_add_cart_time", "u1_latest_buy_time",
      "u2_click_count", "u2_collect_count", "u2_add_cart_count", "u2_buy_count",
      "u3_click_count_pc", "u3_collect_count_pc", "u3_add_cart_count_pc",
      "avg_click", "avg_collect", "avg_add_cart", "avg_buy", "std_click", "std_collect", "std_add_cart", "std_buy", "i1_click_cnt", "i1_collect_cnt", "i1_addcart_cnt", "i1_buy_cnt",
      "i2_buy_click_pc", "i2_buy_collect_pc", "i2_buy_addcat_pc",
      "avg_click_cnt", "std_click_cnt", "avg_collect_cnt", "std_collect_cnt",
      "avg_addcart_cnt", "std_addcart_cnt", "avg_buy_cnt", "std_buy_cnt",
      "ui2_click_cnt", "ui2_favorite_cnt", "ui2_cart_cnt", "ui2_purchase_cnt", "c1_click_cnt", "c1_favorite_cnt", "c1_cart_cnt", "c1_purchase_cnt",
      "c2_click_favorite_pc", "c2_click_cart_pc", "c2_click_purchase_pc",
      "f1_diff_click_time", "f1_diff_favorite_time", "f1_diif_cart_time", "f1_diff_purchase_time",
      "f2_diff_click_time", "f2_diff_favorite_time", "f2_diif_cart_time", "f2_diff_purchase_time",
      "f3_diff_click", "f3_diff_favorite", "f3_diif_cart", "f3_diff_purchase",
      "f4_diff_click", "f4_diff_favorite", "f4_diif_cart", "f4_diff_purchase",
      "f5_ratio_click", "f5_ratio_favorite", "f5_ratio_cart", "f5_ratio_purchase",
      "f8_ratio_click", "f8_ratio_favorite", "f8_ratio_cart", "f8_ratio_purchase"
    )
    //selected feature Index
    val featureIdx = Array(3, 4, 5, 7, 8, 10, 11, 14, 23, 43, 44, 56)
    var featureNames= featureIdx.map(features(_))

    val assembler = new VectorAssembler().setInputCols(featureNames).setOutputCol("features")
    //数据标准化
    val scaler = new StandardScaler().setInputCol("features").setOutputCol("features_scaler").setWithStd(true).setWithMean(false)
    val pie = new Pipeline().setStages(Array(assembler,scaler))
    val dataFrame = pie.fit(train).transform(train)
    //dataFrame.cache()
    // 将数据划分为训练集和测试集
    val Array(trainingData, testData) = dataFrame.randomSplit(Array(0.7, 0.3), seed = 1234L)
    //5.创建Logistic Regression模型：
    val lr = new LogisticRegression().setLabelCol("label").setFeaturesCol("features_scaler").setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    //6.训练模型
    val lrModel = lr.fit(trainingData)
    //7.使用测试集评估模型性能
    val predictions = lrModel.transform(trainingData)
    val prediction = predictions.select("prediction", "label").rdd.map(row => (row.getDouble(0), row.getDouble(1)))
    val metrics = new BinaryClassificationMetrics(prediction)
    ModelScore(metrics,"LR")

  }

  def LRtrain(): Unit = {
    //获取数据集
    val data = spark.read.orc(featurePath)
    //3.将特征列合并成一个向量：
    val assembler = new VectorAssembler()
      .setInputCols(Array("features"))
      .setOutputCol("featuresVector")
      .transform(data)
    //4.将数据集拆分成训练集和测试集：
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), seed = 1234L)
    //5.创建Logistic Regression模型：
    val lr = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("featuresVector")
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
    //6.训练模型
    val lrModel = lr.fit(trainingData)
    //7.使用测试集评估模型性能
    val predictions = lrModel.transform(testData)
    // 评估模型性能
    val prediction = predictions.select("prediction", "label").rdd.map(row => (row.getDouble(0), row.getDouble(1)))
    val metrics = new BinaryClassificationMetrics(prediction)

    val precision = metrics.precisionByThreshold
    precision.foreach { case (t, p) =>
      // 0，1标签的精准率
      println(s"LR-Threshold: $t, Precision: $p")
    }

    // Recall by threshold
    val recall = metrics.recallByThreshold
    recall.foreach { case (t, r) =>
      // 0，1标签的召回率
      println(s"LR-Threshold: $t, Recall: $r")
    }

    // F-measure
    val f1Score = metrics.fMeasureByThreshold
    f1Score.foreach { case (t, f) =>
      println(s"LR-hreshold: $t, F-score: $f, Beta = 1")
    }
  }

  def RFtrain(): Unit = {
    //获取数据集
    val data = spark.read.orc(featurePath)
    //3.将特征列合并成一个向量：
    val assembler = new VectorAssembler()
      .setInputCols(Array("features"))
      .setOutputCol("featuresVector")
      .transform(data)
    //4.将数据集拆分成训练集和测试集：
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), seed = 1234L)
    //5.构建RF模型
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features_vector")
      .setNumTrees(10)
      
    //6.训练模型
    val model = rf.fit(trainingData)
    //7.对测试集进行预测
    val predictions = model.transform(testData)
    //8.评估模型性能
    // 评估模型性能
    val prediction = predictions.select("prediction", "label").rdd.map(row => (row.getDouble(0), row.getDouble(1)))
    val metrics = new BinaryClassificationMetrics(prediction)
    ModelScore(metrics,"RF")
    // 查看模型
    model.toDebugString

    val precision = metrics.precisionByThreshold
    precision.foreach { case (t, p) =>
      // 0，1标签的精准率
      println(s"RF-Threshold: $t, Precision: $p")
    }

    // Recall by threshold
    val recall = metrics.recallByThreshold
    recall.foreach { case (t, r) =>
      // 0，1标签的召回率
      println(s"RF-Threshold: $t, Recall: $r")
    }

    // F-measure
    val f1Score = metrics.fMeasureByThreshold
    f1Score.foreach { case (t, f) =>
      println(s"RF-hreshold: $t, F-score: $f, Beta = 1")
    }

  }

  def GBDTTtrain(): Unit = {
    // 加载数据集data
    val data: DataFrame = spark.read.orc(featurePath)
    // 特征向量列
    val featureCols: Array[String] = data.columns.filter(_ != "user_id")
      .filter(_ != "item_id")
      .filter(_ != "label")
    // 特征向量列转换为特征向量
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
    // 划分数据集为训练集和测试集
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
    // 构建GBDT模型
    val gbdt = new GBTClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(10)
      .setFeatureSubsetStrategy("auto")
      .setMinInfoGain(0.2)
      .setLossType("leastSquares")
    // 构建pipeline
    val pipeline = new Pipeline()
      .setStages(Array(assembler, gbdt))
    // 训练模型
    val model = pipeline.fit(trainingData)
    // 对测试集进行预测
    val predictions = model.transform(testData)
    //保存模型
    model.save("/opt/model/gbdt")
    
    // 评估模型性能
    val prediction = predictions.select("prediction", "label").rdd.map(row => (row.getDouble(0), row.getDouble(1)))
    val metrics = new BinaryClassificationMetrics(prediction)
    ModelScore(metrics,"GBDT")


  }

  def ModelScore(metrics:BinaryClassificationMetrics,name: String): Unit ={
    val precision = metrics.precisionByThreshold
    precision.foreach { case (t, p) =>
      // 0，1标签的精准率
      println(s"$name-Threshold: $t, Precision: $p")
    }

    // Recall by threshold
    val recall = metrics.recallByThreshold
    recall.foreach { case (t, r) =>
      // 0，1标签的召回率
      println(s"$name-Threshold: $t, Recall: $r")
    }

    // F-measure
    val f1Score = metrics.fMeasureByThreshold
    f1Score.foreach { case (t, f) =>
      println(s"$name-hreshold: $t, F-score: $f, Beta = 1")
    }

  }

}
