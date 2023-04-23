package com.cqyt.paper.config

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object SparkInitiation {

  private def getSpark(): SparkSession = {
    //get SparkSession
    if (spark == null) {
      spark = SparkSession.builder().config(sparkConf).getOrCreate()
    }
    spark

  }

  private def getSparkConf(): SparkConf = {
    //get SparkContext
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("userBehavior")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.adaptive.merge.output.small.files.enabled", "true")
      .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "1000")
      .set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
      .set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128mb")
      .set("spark.dynamicAllocation.enabled", "true") // 动态申请资源
      .set("spark.dynamicAllocation.shuffleTracking.enabled", "true") // shuffle动态跟踪
      .set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
      .set("spark.reducer.maxSizeInFlight", "96m")
      .set("spark.sql.shuffle.partitions", "10")
    conf
  }

  //DataBase propertyes
  val map: mutable.Map[String, String] = mutable.HashMap(
    "url" -> "jdbc:mysql://121.37.86.100:3306/taobao?serverTimezone=GMT%2B8&useSSL=false&characterEncoding=utf-8&allowPublicKeyRetrieval=true",
    "driver" -> "com.mysql.cj.jdbc.Driver",
    "user" -> "root",
    "password" -> "123456"
  )
  //
  val sparkConf: SparkConf = getSparkConf()
  val sc: SparkContext = new SparkContext(sparkConf)
  //
  var spark: SparkSession = getSpark()
  val inpath: String = "file:///home/data/orc/"
  val partionInpatn: String = "file:///opt/data"
}
