package ch7

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel

object Optimizing {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Optimizing")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext

    val df =
      spark.range(1 * 10000000).toDF("id").withColumn("square", $"id" * $"id")
    df.show(5)

    // df.cache()
    //
    // val startTime = System.currentTimeMillis()
    // df.count()
    // val endTime = System.currentTimeMillis()
    // val passedTime = (endTime - startTime) * 0.001
    // println(s"took: $passedTime sec")
    //
    // val startTime2 = System.currentTimeMillis()
    // df.count()
    // val endTime2 = System.currentTimeMillis()
    // val passedTime2 = (endTime2 - startTime2) * 0.001
    // println(s"took: $passedTime2 sec")

    // Create a DataFrame with 10M records
    val df2 =
      spark.range(1 * 10000000).toDF("id").withColumn("square", $"id" * $"id")
    df.persist(StorageLevel.DISK_ONLY_2) // ser and cache on disk

    val startTime4 = System.currentTimeMillis()
    df.count()
    val endTime4 = System.currentTimeMillis()
    val passedTime4 = (endTime4 - startTime4) * 0.001
    println(s"took: $passedTime4 sec")

    val startTime5 = System.currentTimeMillis()
    df.count()
    val endTime5 = System.currentTimeMillis()
    val passedTime5 = (endTime5 - startTime5) * 0.001
    println(s"took: $passedTime5 sec")

    df.createTempView("dfCachedTable")
    spark.sql("CACHE TABLE dfCachedTable")
    spark.sql("SELECT count(*) FROM dfCachedTable").show()

    scala.io.StdIn.readLine()

  }
}
