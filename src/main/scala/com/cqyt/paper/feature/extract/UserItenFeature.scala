package com.cqyt.paper.feature.extract

import com.cqyt.paper.config.SparkInitiation.spark
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object UserItenFeature {

  def main(args: Array[String]): Unit = {
    build()
  }

  def build(): Unit ={
    // 读取数据集
    val userBehavior = spark.read.orc("F:\\partiton_uh").filter(col("date")<  "2017-12-02")
    userBehavior.cache()
    // 定义窗口函数
    val w1 = Window.partitionBy("user_id", "item_id").orderBy(desc("timestamp"))
    val w2 = Window.partitionBy("user_id", "item_category")

    // 一、用户-商品特征
    // 1.1 用户对商品点击、收藏、加购物车、购买的最近时间
    val latestTimeDF = userBehavior
      .withColumn("rank", row_number().over(w1))
      .filter("rank = 1")
      .groupBy("user_id", "item_id")
      .agg(
        max(when(col("behavior_type") === 1, col("timestamp"))).as("ui1_latest_click_time"),
        max(when(col("behavior_type") === 2, col("timestamp"))).as("ui1_latest_favorite_time"),
        max(when(col("behavior_type") === 3, col("timestamp"))).as("ui1_latest_cart_time"),
        max(when(col("behavior_type") === 4, col("timestamp"))).as("ui1_latest_purchase_time")
      )
    latestTimeDF.write.mode(SaveMode.Overwrite).orc("file:///F:/feature/userItem/userItemBehavior_latest")

    // 1.2 用户对商品点击、收藏、加购物车、购买的次数
    val countDF = userBehavior
      .groupBy("user_id", "item_id")
      .agg(
        sum(when(col("behavior_type") === 1, 1)).as("ui2_click_cnt"),
        sum(when(col("behavior_type") === 2, 1)).as("ui2_favorite_cnt"),
        sum(when(col("behavior_type") === 3, 1)).as("ui2_cart_cnt"),
        sum(when(col("behavior_type") === 4, 1)).as("ui2_purchase_cnt")
      )
    countDF.write.mode(SaveMode.Overwrite).orc("file:///F:/feature/userItem/userItemBehavior_cnt")

    userBehavior.unpersist()
  }

}
