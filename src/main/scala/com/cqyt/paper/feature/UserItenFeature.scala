package com.cqyt.paper.feature

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
    val userBehavior = spark.read.orc("file:///opt/data")

    // 定义窗口函数
    val w1 = Window.partitionBy("user_id", "item_id").orderBy(desc("date"), desc("hour"))
    val w2 = Window.partitionBy("user_id", "item_category")

    // 一、用户-商品特征
    // 1.1 用户对商品点击、收藏、加购物车、购买的最近时间
    val latestTimeDF = userBehavior
      .filter(col("date")<= "2014-12-17")
      .withColumn("rank", row_number().over(w1))
      .filter("rank = 1")
      .groupBy("user_id", "item_id")
      .agg(
        max(when(col("behavior_type") === 1, concat_ws(" ", col("date"), col("hour")))).as("ui1_latest_click_time"),
        max(when(col("behavior_type") === 2, concat_ws(" ", col("date"), col("hour")))).as("ui1_latest_favorite_time"),
        max(when(col("behavior_type") === 3, concat_ws(" ", col("date"), col("hour")))).as("ui1_latest_cart_time"),
        max(when(col("behavior_type") === 4, concat_ws(" ", col("date"), col("hour")))).as("ui1_latest_purchase_time")
      )
    latestTimeDF.coalesce(50).write.mode(SaveMode.Overwrite).orc("file:///opt/feature/userItem/userItemBehavior_latest")

    // 1.2 用户对商品点击、收藏、加购物车、购买的次数
    val countDF = userBehavior
      .filter(col("date") <= "2014-12-17")
      .groupBy("user_id", "item_id")
      .agg(
        sum(when(col("behavior_type") === 1, 1)).as("ui2_click_cnt"),
        sum(when(col("behavior_type") === 2, 1)).as("ui2_favorite_cnt"),
        sum(when(col("behavior_type") === 3, 1)).as("ui2_cart_cnt"),
        sum(when(col("behavior_type") === 4, 1)).as("ui2_purchase_cnt")
      )
    countDF.coalesce(50).write.mode(SaveMode.Overwrite).orc("file:///opt/feature/userItem/userItemBehavior_cnt")
  }

}
