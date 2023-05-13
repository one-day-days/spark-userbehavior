package com.cqyt.paper.feature.selection

import com.cqyt.paper.config.SparkInitiation.spark
import com.cqyt.paper.config.FeatureConfig
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.col

object FeatureCombine {
  def main(args: Array[String]): Unit = {

    val toSeconds = 1000L
    //读取数据
    val train = spark.read.orc("F://train")
    val userBehavior_latset = spark.read.orc(FeatureConfig.userBehavior_latset)
      .withColumn("u1_latest_click_time", col("u1_latest_click_time")/ toSeconds)
      .withColumn("u1_latest_collect_time",col("u1_latest_collect_time")/ toSeconds)
      .withColumn("u1_latest_add_cart_time", col("u1_latest_add_cart_time") / toSeconds)
      .withColumn("u1_latest_buy_time",col("u1_latest_buy_time")/ toSeconds)
    val userBehavior_count = spark.read.orc(FeatureConfig.userBehavior_count)
    val userBehavior_percetconv = spark.read.orc(FeatureConfig.userBehavior_percetconv)
    val userBehavior_std = spark.read.orc(FeatureConfig.userBehavior_std)
    val comdity_click_collect_addcart_buy_cnt = spark.read.orc(FeatureConfig.comdity_click_collect_addcart_buy_cnt)
    val comdity_click_collect_addcart_buy_pc = spark.read.orc(FeatureConfig.comdity_click_collect_addcart_buy_pc)
    val comdity_click_collect_addcart_buy_std = spark.read.orc(FeatureConfig.comdity_click_collect_addcart_buy_std)
    val userItemBehavior_latest = spark.read.orc(FeatureConfig.userItemBehavior_latest)
      .withColumn("ui1_latest_click_time", col("ui1_latest_click_time")/ toSeconds)
      .withColumn("ui1_latest_favorite_time",col("ui1_latest_favorite_time")/ toSeconds)
      .withColumn("ui1_latest_cart_time", col("ui1_latest_cart_time") / toSeconds)
      .withColumn("ui1_latest_purchase_time",col("ui1_latest_purchase_time")/ toSeconds)
    val userItemBehavior_cnt = spark.read.orc(FeatureConfig.userItemBehavior_cnt)
    val categoryBehaviorCnt = spark.read.orc(FeatureConfig.categoryBehaviorCnt)
    val categoryBehaviorPC = spark.read.orc(FeatureConfig.categoryBehaviorPC)
    val f8 = spark.read.orc(FeatureConfig.f8)
    val f1 = spark.read.orc(FeatureConfig.f1)
    val f2 = spark.read.orc(FeatureConfig.f2)
    val f3 = spark.read.orc(FeatureConfig.f3)
    val f4 = spark.read.orc(FeatureConfig.f4)
    val f5 = spark.read.orc(FeatureConfig.f5)
    // 合并特征数据为一个DataFrame
    val features = train.join(userBehavior_latset,Seq("user_id"),"left")
      .join(userBehavior_count, Seq("user_id"),"left")
      .join(userBehavior_percetconv, Seq("user_id"),"left")
      .join(userBehavior_std, Seq("user_id"),"left")
      .join(comdity_click_collect_addcart_buy_cnt, Seq("item_id"),"left")
      .join(comdity_click_collect_addcart_buy_pc, Seq("item_id"),"left")
      .join(comdity_click_collect_addcart_buy_std, Seq("item_id"),"left")
      .join(userItemBehavior_cnt,Seq("user_id", "item_id"),"left")
      .join(userItemBehavior_latest,Seq("user_id", "item_id"),"left")
//      .join(userItemCategoryRatio, Seq("user_id", "item_id"))
      .join(categoryBehaviorCnt, Seq("item_category"),"left")
      .join(categoryBehaviorPC, Seq("item_category"),"left")
      .join(f8, Seq("item_id"),"left")
      .join(f1, Seq("user_id", "item_id"),"left")
      .join(f2, Seq("user_id", "item_id"),"left")
      .join(f3, Seq("user_id", "item_id"),"left")
      .join(f4, Seq("user_id", "item_id"),"left")
      .join(f5, Seq("user_id","item_id"),"left")
    // 转换列类型
    val dfDouble = features.select(features.columns.map(c => col(c).cast("double")): _*)
    //填补空值
    val df = dfDouble.na.fill(0,FeatureConfig.featureColmuns)
    //保存合并后的特征
    df.write.mode(SaveMode.Overwrite).orc("F://featureCombine")
  }

}
