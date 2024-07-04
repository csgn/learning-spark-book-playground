package ch3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Struct {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Struct")
      .getOrCreate()

    spark.stop()
  }
}
