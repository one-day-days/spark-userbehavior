package com.cqyt.paper.feature.extract

import com.cqyt.paper.config.SparkInitiation.spark
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.col

object IntersectFeature {

  def main(args: Array[String]): Unit = {
    buildF1()
    buildF2()
    buildF3()
    buildF4()
    buildF5()
    buildF8()
  }

  //用户最近点击、收藏、加购物车、购买时间
  lazy val userBehaviorLatest = spark.read.orc("file:///F:/feature/user/userBehavior_latest")
  //用户对商品最近点击、收藏、加购物车、购买时间
  lazy val userItemLatest = spark.read.orc("file:///F:/feature/userItem/userItemBehavior_latest")
  //用户平均点击、收藏、加购物车、购买量
  lazy val userBehaviorAvg = spark.read.orc("file:///F:/feature/user/userBehavior_Std")
  //用户对商品点击、收藏、加购物车、购买的次数
  lazy val userItemCnt = spark.read.orc("file:///F:/feature/userItem/userItemBehavior_cnt")
  //商品平均被点击、收藏、加购物车、购买量
  lazy val ItenBehaviorAvg = spark.read.orc("file:///F:/feature/commodity/comdity_click_collect_addcart_buy_std")
  //商品被点击、收藏、加购物车、购买量
  lazy val itemBehaviorCnt = spark.read.orc("file:///F:/feature/commodity/comdity_click_collect_addcart_buy_cnt")
    .withColumnRenamed("click_cnt","item_click_cnt")
  //每个商品被点击、收藏、加购物车、购买量
  lazy val categoryBehaviorCount=  spark.read.orc("file:///F:/feature/category/categoryBehaviorCnt")
  //用户点击、收藏、加购物车、购买量
  lazy val userBehaviorCnt = spark.read.orc("file:///F:/feature/user/userBehavior_count")

  def buildF8(): Unit ={
    //8、商品被点击、收藏、加购物车、购买量除以该类商品被点击、收藏、加购物车、购买量
    val f8 = itemBehaviorCnt.join(categoryBehaviorCount,Seq("item_category"),"left")
      .select(
        col("item_id"),
        col("i1_click_cnt") / col("c1_click_cnt") as "f8_ratio_click",
        col("i1_collect_cnt") / col("c1_favorite_cnt") as "f8_ratio_favorite",
        col("i1_addcart_cnt") / col("c1_cart_cnt") as "f8_ratio_cart",
        col("i1_buy_cnt") / col("c1_purchase_cnt") as "f8_ratio_purchase"
      )
    f8.write.mode(SaveMode.Overwrite).orc("file:///F:/feature/intersect/f8")

  }
  def buildF5(): Unit ={
    //5、用户对商品点击、收藏、加购物车、购买量除以用户点击、收藏、加购物车、购买量
    val f5= userItemCnt.join(userBehaviorCnt, Seq("user_id"), "left").select(
      col("user_id"), col("item_id"),
      col("ui2_click_cnt") / col("u2_click_count") as "f5_ratio_click",
      col("ui2_favorite_cnt") / col("u2_collect_count") as "f5_ratio_favorite",
      col("ui2_cart_cnt") / col("u2_add_cart_count") as "f5_ratio_cart",
      col("ui2_purchase_cnt") / col("u2_buy_count") as "f5_ratio_purchase"
    )
    f5.write.mode(SaveMode.Overwrite).orc("file:///F:/feature/intersect/f5")
  }
  def buildF4(): Unit ={
    //4.用户对商品点击、收藏、加购物车、购买量减去商品平均点击、收藏、加购物车、购买量
    val f4 = userItemCnt.join(ItenBehaviorAvg,Seq("item_id"),"left").select(
      col("user_id"), col("item_id"),
      col("ui2_click_cnt") - col("avg_click_cnt") as "f4_diff_click",
      col("ui2_favorite_cnt") - col("avg_collect_cnt") as "f4_diff_favorite",
      col("ui2_cart_cnt") - col("avg_addcart_cnt") as "f4_diif_cart",
      col("ui2_purchase_cnt") - col("avg_buy_cnt") as "f4_diff_purchase"
    )
    f4.write.mode(SaveMode.Overwrite).orc("file:///F:/feature/intersect/f4")


  }
  def buildF3(): Unit = {
    //3.用户对商品点击、收藏、加购物车、购买量减去用户平均点击、收藏、加购物车、购买量
    val f3 = userItemCnt.join(userBehaviorAvg, Seq("user_id"), "left").select(
      col("user_id"), col("item_id"),
      col("ui2_click_cnt") - col("avg_click") as "f3_diff_click",
      col("ui2_favorite_cnt") - col("avg_collect") as "f3_diff_favorite",
      col("ui2_cart_cnt") - col("avg_add_cart") as "f3_diif_cart",
      col("ui2_purchase_cnt") - col("avg_buy") as "f3_diff_purchase"
    )
    f3.write.mode(SaveMode.Overwrite).orc("file:///F:/feature/intersect/f3")
  }

  def buildF2(): Unit = {
    //2.用户对商品最近点击、收藏、加购物车、购买时间减去该用户购买时间 userItemLatest
    val f2 = userItemLatest.join(userBehaviorLatest, Seq("user_id"), "left").select(
      col("user_id"), col("item_id"),
      col("ui1_latest_click_time") - col("u1_latest_buy_time") as "f2_diff_click_time",
      col("ui1_latest_favorite_time") - col("u1_latest_buy_time") as "f2_diff_favorite_time",
      col("ui1_latest_cart_time") - col("u1_latest_buy_time") as "f2_diif_cart_time",
      col("ui1_latest_purchase_time") - col("u1_latest_buy_time") as "f2_diff_purchase_time"
    )
    f2.write.mode(SaveMode.Overwrite).orc("file:///F:/feature/intersect/f2")
  }
  def buildF1(): Unit ={
    //1、用户对商品最近点击、收藏、加购物车、购买时间减去该用户最近点击、收藏、加购物车、购买时间
    val f1 = userItemLatest.join(userBehaviorLatest,Seq("user_id"),"left").select(
      col("user_id"),col("item_id"),
      col("ui1_latest_click_time") - col("u1_latest_click_time") as "f1_diff_click_time",
      col("ui1_latest_favorite_time") - col("u1_latest_collect_time") as "f1_diff_favorite_time",
      col("ui1_latest_cart_time") - col("u1_latest_add_cart_time") as "f1_diif_cart_time",
      col("ui1_latest_purchase_time") - col("u1_latest_buy_time") as "f1_diff_purchase_time"
    )
    f1.write.mode(SaveMode.Overwrite).orc("file:///F:/feature/intersect/f1")

  }

}
