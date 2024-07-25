package ch5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object ExternalSources {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("ExternalSources")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext

    val cubed = (s: Long) => {
      s * s * s
    }

    spark.udf.register("cubed", cubed)

    spark.range(1, 9).createOrReplaceTempView("udf_test")

    spark.sql("SELECT id, cubed(id) as id_cubed FROM udf_test").show()

  }
}
