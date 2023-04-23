package com.cqyt.paper.data

import com.cqyt.paper.config.SparkInitiation.sc
import com.cqyt.paper.config.SparkInitiation.spark
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Commdity {

  def main(args: Array[String]): Unit = {
    test2()
  }


  def test1(): Unit ={
    // 读取文本文件
    val textFileRDD = sc.textFile("file:///home/WUYING_438857854_1920906118/Downloads")

    // 定义schema
    val schema = StructType(
      Array(
        StructField("name", StringType, true),
        StructField("age", IntegerType, true),
        StructField("country", StringType, true)
      )
    )
    // 将每行数据转换为Row
    val rowRDD = textFileRDD.map(line => {
      val fields = line.split("\t")
      Row(fields(0), fields(1).toInt, fields(2))
    })
    // 创建DataFrame
    val df = spark.createDataFrame(rowRDD, schema)
    // 显示DataFrame
    df.show()
  }

  def test2(): Unit ={
    val df = spark.read
      .option("header", false)
      .option("delimiter", "\t")
      .option("inferSchema", true)
      .csv("file:///home/WUYING_438857854_1920906118/Downloads/tianchi_fresh_comp_train_item_online.txt")
      .toDF("item_id", "item_ geohash", "item_category")
    df.createOrReplaceTempView("commdity")
    val distcomm = spark.sql("select item_id,min(item_category)  item_category from commdity group by item_id")
    distcomm.coalesce(1).write.mode(SaveMode.Overwrite).orc("file:///home/data/commdity")

  }
}
