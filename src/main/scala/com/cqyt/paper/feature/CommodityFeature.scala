package com.cqyt.paper.feature

import com.cqyt.paper.config.SparkInitiation.spark
import org.apache.spark.sql.SaveMode

object CommodityFeature {

  def main(args: Array[String]): Unit = {
    buildFeature()
    System.in.read()
    spark.stop()
  }

  def buildFeature(){

    val df = spark.read.orc("file:///opt/data")
    df.cache()
    df.createOrReplaceTempView("userBehavior")
    //商品被点击、收藏、加购物车、购买量
    val sql1 = """|SELECT item_id,
                 |       COUNT(CASE WHEN behavior_type = 1 THEN 1 END) AS i1_click_cnt,
                 |       COUNT(CASE WHEN behavior_type = 2 THEN 1 END) AS i1_collect_cnt,
                 |       COUNT(CASE WHEN behavior_type = 3 THEN 1 END) AS i1_addcart_cnt,
                 |       COUNT(CASE WHEN behavior_type = 4 THEN 1 END) AS i1_buy_cnt
                 |FROM userBehavior
                 |WHERE date <= '2014-12-17'
                 |GROUP BY item_id;""".stripMargin
    val frame = spark.sql(sql1)
    frame.cache()
    frame.coalesce(4).write.mode(SaveMode.Overwrite).orc("file:///opt/feature/commodity/comdity_click_collect_addcart_buy_cnt")
    frame.createOrReplaceTempView("item_behavior_cnt")

    //商品被购买转化率
    val sql2 = """|SELECT item_id,
                 |       (i1_buy_cnt / i1_click_cnt) as i2_buy_click_pc,
                 |       (i1_buy_cnt / i1_collect_cnt) as i2_buy_collect_pc,
                 |       (i1_buy_cnt / i1_addcart_cnt) as i2_buy_addcat_pc
                 |FROM item_behavior_cnt;""".stripMargin
    spark.sql(sql2).write.mode(SaveMode.Overwrite).orc("file:///opt/feature/commodity/comdity_click_collect_addcart_buy_pc")

    //商品被点击、收藏、加购物车、购买量在28天里的均值方差
    val sql3 = """|SELECT item_id,
                 |       AVG(i1_click_cnt) AS avg_click_cnt,
                 |       STDDEV_SAMP(i1_click_cnt) AS std_click_cnt,
                 |       AVG(i1_collect_cnt) AS avg_collect_cnt,
                 |       STDDEV_SAMP(i1_collect_cnt) AS std_collect_cnt,
                 |       AVG(i1_addcart_cnt) AS avg_addcart_cnt,
                 |       STDDEV_SAMP(i1_addcart_cnt) AS std_addcart_cnt,
                 |       AVG(i1_buy_cnt) AS avg_buy_cnt,
                 |       STDDEV_SAMP(i1_buy_cnt) AS std_buy_cnt
                 |FROM item_behavior_cnt
                 |GROUP BY item_id;""".stripMargin
    spark.sql(sql3).coalesce(5).write.mode(SaveMode.Overwrite).orc("file:///opt/feature/commodity/comdity_click_collect_addcart_buy_std")
    frame.unpersist()


  }



}
