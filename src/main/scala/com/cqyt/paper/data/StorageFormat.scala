package com.cqyt.paper.data

import com.cqyt.paper.config.SparkInitiation.spark
object StorageFormat {

  def main(args: Array[String]): Unit = {
    val df = spark.read.option("header","ture").csv("/opt/data/csv")
    df.write.orc("/opt/data/orc")
    df.write.parquet("/opt/data/parquet")
  }

}
