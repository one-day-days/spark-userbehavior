package com.cqyt.paper.feature.extract

import com.cqyt.paper.config.SparkInitiation.spark
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, sum}

object UserCmdtCategory {
  def main(args: Array[String]): Unit = {
    build()
  }

  def build() {
    //1.用户对商品的点击、收藏、加购物车、购买次数除用户对该商品所属分类（item_category）的点击、收藏、加购物车、购买次数。
    val categoryItem = spark.read.orc("file:///F:/userbehavior/commdity")
    val userItemBehavior = spark.read.orc("file:///F:/feature/userItem/userItemBehavior_cnt")

    val category_item_cnt = userItemBehavior
      .join(categoryItem, Seq("item_id"), "left")
      .select("user_id","item_category", "item_id", "ui2_click_cnt", "ui2_favorite_cnt", "ui2_cart_cnt", "ui2_purchase_cnt")
    //用户各商品种类（item_category）的点击、收藏、加购物车、购买次数。
    val category_cnt = category_item_cnt.groupBy("user_id","item_category").agg(
      sum(col("ui2_click_cnt")).as("category_click_cnt"),
      sum(col("ui2_favorite_cnt")).as("category_favorite_cnt"),
      sum(col("ui2_cart_cnt")).as("category_cart_cnt"),
      sum(col("ui2_purchase_cnt")).as("category_purchase_cnt")
    )
    //
    val userItemCategoryRatio = category_cnt.join(category_item_cnt, Seq("user_id","item_category"),"inner").select(
      col("user_id"),col("item_id"),col("item_category"),
      col("ui2_click_cnt") / col("category_click_cnt") as " click_ratio",
      col("ui2_favorite_cnt") / col("category_favorite_cnt") as "favorite_ratio",
      col("ui2_cart_cnt") / col("category_cart_cnt") as "cart_ratio",
      col("ui2_purchase_cnt") / col("category_purchase_cnt") as "purchase_ratio"
    )
    userItemCategoryRatio.write.mode(SaveMode.Overwrite).orc("file:///F:/feature/userItemCategoryRatio")
    //2、用户对该商品的最近点击、收藏、加购物车、购买时间减去同类其他商品的最近点击、收藏、加购物车、购买时间


  }
}
