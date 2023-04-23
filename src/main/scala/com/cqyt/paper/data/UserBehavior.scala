package com.cqyt.paper.data

import com.cqyt.paper.config.SparkInitiation
import com.cqyt.paper.config.SparkInitiation.{inpath, sc, spark}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, sum, count}

object UserBehavior {

  /*
   * 将原始数据集通过orc格式存储snappy压缩
   * @param inpath  file:///opt/data/bzip2/
   * @param outpath file:///opt/data/orc
   */
  def initDataSet(inpath: String, outpath: String = inpath): Unit = {

    val spark = SparkInitiation.spark
    val ubData = sc.textFile(inpath)
    val rdd1 = ubData.map(x => x.split("\t")).map(x => {
      (x(0), x(1), x(2), x(3), x(4), x(5).split(" ")(0), x(5).split(" ")(1))
    })
    //
    import spark.implicits._
    //
    val df = rdd1.toDF("user_id", "item_id", "behavior_type", "user_geohash", "item_category", "date", "hour")
    //
    df.coalesce(90).write.format("orc").mode(SaveMode.Overwrite).save(outpath)
    print("SUCCEFUL")

  }

  def getDatePageView(inpath: String = inpath): Unit = {
    val df = spark.read.orc(inpath)
    val pv = df.groupBy("date").count().orderBy("date")
    val datePV = pv.withColumnRenamed("count", "pv")
    datePV.show()
    datePV.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv("file:///home/data/date_pv")

  }

  def getDateUniqueVisitor(inpath: String = inpath): Unit = {

    val df = spark.read.orc(inpath)
    df.createOrReplaceTempView("user")
    val sql =
      """
				|select date,count(DISTINCT(user_id)) uv
				|from user
				|group by date
				|""".stripMargin
    val dateUV = spark.sql(sql)

    dateUV.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv("file:///home/data/date_uv")
  }

  def getHourPageView(inpath: String = inpath): Unit = {
    val df = spark.read.orc(inpath)
    val pv = df.groupBy("hour").count().orderBy("hour")
    val datePV = pv.withColumnRenamed("count", "pv").orderBy("hour")
    datePV.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv("file:///home/data/hour_pv")
  }

  def getUserCount(inpath: String = inpath): Unit = {

    val df = spark.read.orc(inpath).filter(col("date") === "2014-12-18")
    val users = df.select("item_id").distinct().count()
    print(users)
  }

  def getHourUniqueVisitor(inpath: String = inpath): Unit = {

    val df = spark.read.orc(inpath)
    df.createOrReplaceTempView("user")
    val sql =
      """
				|select hour,count(DISTINCT(user_id)) uv
				|from user
				|group by hour
				|""".stripMargin
    val dateUV = spark.sql(sql).orderBy("hour")
    dateUV.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv("file:///home/data/hour_uv")
  }

  //计算各种用户行为在不同时间的次数
  def getBehaviorHourPageView(inpath: String = inpath): Unit = {

    val df = spark.read.orc(inpath)
    val pv = df.groupBy("behavior_type", "hour").count().orderBy("behavior_type", "hour")
    val behaviorPV = pv.withColumnRenamed("count", "pv")
    behaviorPV.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv("file:///home/data/behavior_hour_uv")

  }

  //Statistics of various user behaviors within a month
  def sumBehaviorTypes(inpath: String = "file:///home/data/behavior_hour_uv"): Unit = {

    val df = spark.read.option("header", "true").csv(inpath)
    val pv = df.groupBy("behavior_type").agg(sum("pv") as "count").orderBy("behavior_type")
    pv.show()
    pv.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv("file:///home/data/behaviorType_count")
  }

  //计算12-12每种用户行为次数
  def sumBehaviorTypes12(inpath: String = inpath): Unit = {

    val df = spark.read.orc(inpath)
    df.createOrReplaceTempView("user")
    //		val behaviorPV = pv.withColumnRenamed("count", "pv")
    val sql = "select behavior_type,count(1) clicks from user where date='2014-12-12' group by behavior_type;"
    val pv = spark.sql(sql)
    pv.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv("file:///home/data/behavior_12_uv")

  }

  //获取每天每位用户的various user behaviors次数
  def getDateUserTypes(inpath: String = inpath): Unit = {

    val df = spark.read.orc(inpath)
    val dataUserArpu = df.groupBy("date", "user_id", "behavior_type").count().orderBy("date").orderBy("user_id")
    dataUserArpu.write.option("header", "true").mode(SaveMode.Overwrite).csv("file:///home/data/date_user_types")
  }

  //计算每日每位用户的购买次数
  def getDateUserBuys(inpath: String = "file:///home/data/date_user_types"): Unit = {
    val df = spark.read.option("header", "true").csv(inpath)
    val dataUserArpu = df.filter("behavior_type=4")
      .groupBy("date", "user_id")
      .agg(sum("count") as "buys")
      .orderBy("date").orderBy("user_id")

    dataUserArpu.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv("file:///home/data/date_user_buys")
  }

  //计算每日购买次数
  def getDateBuys(inpath: String = "file:///home/data/date_user_buys"): Unit = {
    val df = spark.read.option("header", "true").csv(inpath)
    val dataUserArpu = df.groupBy("date")
      .agg(sum("buys") as "buys")
      .orderBy("date")
    dataUserArpu.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv("file:///home/data/date_buys")
  }

  //6.1用户购买频次分析	获取一个月内每位用户的购买次数
  def getMonthUserBuys(inpath: String = "file:///home/data/date_user_buys"): Unit = {
    val df = spark.read.option("header", "true").csv(inpath)
    df.createOrReplaceTempView("user")
    val sql = "select user_id,sum(buys) buys from user group by user_id order by buys desc"
    spark.sql(sql).write.option("header", "true").mode(SaveMode.Overwrite).csv("file:///home/data/month_user_buys")
  }

  //partition write data
  def getPartitionData(inpath: String): Unit = {
    val df = spark.read.orc(inpath)
    df.coalesce(8).write
      .partitionBy("date")
      .mode(SaveMode.Overwrite)
      .bucketBy(4, "user_id")
      .orc("file:///opt/data")
  }

  //获取转化率
  def getConversionRate(): Unit = {
    spark.read.option("header", "true").csv("file:///home/data/")
  }

  //
  def repurchase(): Unit = {
    val dateUserBuys = spark.read.option("header", "true").csv("file:///home/data/date_user_buys")
    val result = dateUserBuys.groupBy("user_id", "date")
      .agg(count(col("buys")).as("count"))
      .filter(col("count") >= 2)
    result.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv("file:///home/data/repurchase")
  }

  def main(args: Array[String]): Unit = {
    //    getPartitionData("file:///opt/module/dataset")
    repurchase()


    if (args.contains("intDataSet")) {
      initDataSet("file:///opt/data/bzip2/")
    }
    if (args.contains("getPV")) {
      getDatePageView()
      getHourPageView()
    }
    if (args.contains("getUV")) {
      getDateUniqueVisitor()
      getHourUniqueVisitor()
    }
    if (args.contains("userCount")) {
      getUserCount()
    }
    if (args.contains("behaviorHourPageView")) {
      getBehaviorHourPageView()
    }
    if (args.contains("sumBehavior")) {
      sumBehaviorTypes()
      sumBehaviorTypes12()
    }
    if (args.contains("dateUserTypes")) {
      getDateUserTypes()
    }
    if (args.contains("dateUserBuys")) {
      getDateUserBuys()
    }
    if (args.contains("dateBuys")) {
      getDateBuys()
    }
    if (args.contains("monthUserBuys")) {
      getMonthUserBuys()
    }
    //		System.in.read()
    spark.stop()

  }
}

