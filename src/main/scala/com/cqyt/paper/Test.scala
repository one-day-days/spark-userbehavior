package com.cqyt.paper

import com.cqyt.paper.config.FeatureConfig
import com.cqyt.paper.config.SparkInitiation.spark
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions.{col, date_format, from_unixtime, max, when}
import org.apache.spark.ml.feature.{ChiSqSelector, VectorSlicer, SQLTransformer, StandardScaler, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode}

object Test {

  def main(args: Array[String]): Unit = {

  

  }

  def createTrain(date:String ="2017-12-02"): Unit = {
    val df = spark.read.orc("F:\\partiton_uh").filter(col("date") === date)
    df.groupBy("user_id", "item_id").agg(
      when(max("behavior_type") === 4, 1).otherwise(0).as("label")
    ).write.option("header", "true").mode(SaveMode.Overwrite).orc("F:\\train")
  }

  def addColumn(): Unit = {
    val df = spark.read.orc("F:\\pureUserBehavior")
    val userBehavior = df.withColumn("date", date_format(from_unixtime(col("timestamp")), "yyyy-MM-dd"))
    //    userBehavior.coalesce(4).write.partitionBy("date").mode(SaveMode.Overwrite).orc("F:\\pureUserBehavior")
    //    userBehavior.coalesce(4).write.parquet("F:\\pureUserBehavior")
    //    val userBehavior = df.withColumn("date", to_date(col("time"), "YYYY-MM-dd"))
    //    userBehavior.groupBy("date").count().as("uv").show(30)
    //    spark.sql("to_date(from_unixtime(col('1511539201')),'yyyy-MM-dd')")
  }

  def columnCast(): Unit = {
    //列值转换为数值，方便计算
    val df = spark.read.orc("F:\\pureUserBehavior")
    df.select(col("user_id"), col("item_id"), col("timestamp"), col("date"), col("item_category"),
      when(col("behavior_type") === "pv", 1)
        .when(col("behavior_type") === "fav", 2)
        .when(col("behavior_type") === "buy", 3)
        .when(col("behavior_type") === "cart", 4)
        .otherwise(0).as("behavior_type"))
      .coalesce(4).write.partitionBy("date")
      .orc("F:\\partiton_uh")

  }




}

