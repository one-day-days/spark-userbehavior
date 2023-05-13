package com.cqyt.paper.feature.extract

import com.cqyt.paper.config.SparkInitiation.spark
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, sum, when}

object CategoryFeature {

  def main(args: Array[String]): Unit = {
    build()
    System.in.read()
  }
  def build(): Unit ={

    //1、该类商品被点击、收藏、加购物车、购买量
    val df = spark.read.orc("F:\\partiton_uh").filter(col("date")<  "2017-12-02")
    val categoryBehaviorCnt = df.groupBy("item_category").agg(
      sum(when(col("behavior_type") === 1, 1)).as("c1_click_cnt"),
      sum(when(col("behavior_type") === 2, 1)).as("c1_favorite_cnt"),
      sum(when(col("behavior_type") === 3, 1)).as("c1_cart_cnt"),
      sum(when(col("behavior_type") === 4, 1)).as("c1_purchase_cnt")
    )
    categoryBehaviorCnt.cache()
    categoryBehaviorCnt.coalesce(1).write.mode(SaveMode.Overwrite).orc("file:///F:\\feature\\category\\categoryBehaviorCnt")

    //2、该类商品转化率
    categoryBehaviorCnt.select(
      col("item_category"),
      col("c1_click_cnt") / col("c1_favorite_cnt") as "c2_click_favorite_pc",
      col("c1_click_cnt") / col("c1_cart_cnt") as "c2_click_cart_pc",
      col("c1_click_cnt") / col("c1_purchase_cnt") as "c2_click_purchase_pc"
    ).coalesce(1).write.mode(SaveMode.Overwrite).orc("file:///F:\\feature\\category\\categoryBehaviorPC")
  }

}
