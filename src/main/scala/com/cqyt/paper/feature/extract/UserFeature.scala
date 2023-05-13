package com.cqyt.paper.feature.extract

import com.cqyt.paper.config.SparkInitiation.spark
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object UserFeature {


  def main(args: Array[String]): Unit = {

    buildFeature("2014-11-17")
    spark.stop()
  }

  def buildFeature(date: String): Unit = {

    // 选取"2014-12-18"前5天的数据
    val df = spark.read.orc("F:\\partiton_uh").filter(col("date") < "2017-12-02")
    df.cache()

    //1、用户对商品最近点击、收藏、加购物车、购买时间
    val userBehaviorFeatures = df.groupBy("user_id").agg(
      coalesce(max(when(col("behavior_type") === 1, col("timestamp"))), lit("1511452800")).as("u1_latest_click_time"),
      coalesce(max(when(col("behavior_type") === 2, col("timestamp"))), lit("1511452800")).as("u1_latest_collect_time"),
      coalesce(max(when(col("behavior_type") === 3, col("timestamp"))), lit("1511452800")).as("u1_latest_add_cart_time"),
      coalesce(max(when(col("behavior_type") === 4, col("timestamp"))), lit("1511452800")).as("u1_latest_buy_time")
    )
    userBehaviorFeatures.write.mode(SaveMode.Overwrite).orc("file:///F:/feature/user/userBehavior_latest")

    //2.用户点击、收藏、加购物车、购买量
    val userBehaviorCount = df
      .groupBy("user_id")
      .agg(
        sum(when(col("behavior_type") === 1, 1)).as("u2_click_count"),
        sum(when(col("behavior_type") === 2, 1)).as("u2_collect_count"),
        sum(when(col("behavior_type") === 3, 1)).as("u2_add_cart_count"),
        sum(when(col("behavior_type") === 4,1)).as("u2_buy_count")
      )

    userBehaviorCount.cache()
    userBehaviorCount.write.mode(SaveMode.Overwrite).orc("file:///F:/feature/user/userBehavior_count")

    //3.用户转化率即用户购买量分别除以用户点击、收藏、加购物车这三类行为数
    //user percent conversion
    val userPC = userBehaviorCount.select(col("user_id"),
      col("u2_click_count") / col("u2_collect_count") as "u3_click_count_pc",
      col("u2_click_count") / col("u2_add_cart_count") as "u3_collect_count_pc",
      col("u2_click_count") / col("u2_buy_count") as "u3_add_cart_count_pc")
    userPC.write.mode(SaveMode.Overwrite).orc("file:///F:/feature/user/userBehavior_percetconv")

    //4.用户点击、收藏、加购物车、购买量在28天里的均值方差
    val userBehaviorStd = userBehaviorCount.groupBy("user_id")
      .agg(
        avg(col("u2_click_count")).as("avg_click"),
        avg(col("u2_collect_count")).as("avg_collect"),
        avg(col("u2_add_cart_count")).as("avg_add_cart"),
        avg(col("u2_buy_count")).as("avg_buy"),
        stddev_samp(col("u2_click_count")).as("std_click"),
        stddev_samp(col("`u2_collect_count`")).as("std_collect"),
        stddev_samp(col("`u2_add_cart_count`")).as("std_add_cart"),
        stddev_samp(col("`u2_buy_count`")).as("std_buy")
      )

    userBehaviorStd.write.mode(SaveMode.Overwrite).option("header", "true").orc("file:///F:/feature/user/userBehavior_std")
    userBehaviorCount.unpersist()
    df.unpersist()
  }

}
