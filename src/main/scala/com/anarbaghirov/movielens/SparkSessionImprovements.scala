package com.anarbaghirov.movielens

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

private class SparkSessionImprovements(val s: SparkSession) {
  def loadCSV(path: String): sql.DataFrame = {
    s.read
      .option("inferSchema", "true")
      .option("header", "true")
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .load(path)
      .cache()
  }
}