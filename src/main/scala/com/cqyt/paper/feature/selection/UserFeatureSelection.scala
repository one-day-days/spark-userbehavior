package com.cqyt.paper.feature.selection

import com.cqyt.paper.config.SparkInitiation.spark
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{ChiSqSelector, StandardScaler, VectorAssembler}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.col

object UserFeatureSelection {

  def main(args: Array[String]): Unit = {
    val toSeconds = 1000L
    val df = spark.read.orc("F://featureCombine")
    df.printSchema()
    val count = df.groupBy("label").count()
    count.show()

  }


}
