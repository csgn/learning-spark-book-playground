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

    val s = Seq(
      (1, Seq(1, 2, 3)),
      (2, Seq(4, 5, 6))
    ).toDF("id", "values")

    s.createOrReplaceTempView("table")

    def multiplyN(values: Seq[Int], n: Int): Seq[Int] = {
      values.map(_ * n)
    }

    spark.udf.register("multiplyN", multiplyN(_, _))

    spark
      .sql("""
      SELECT
        id, collect_list(value * 2) as values
      FROM (
        SELECT
          id, EXPLODE(values) as value
        FROM table
      ) x
      GROUP BY id
    """)
      .show()

    spark
      .sql("""
      SELECT
        id, multiplyN(values, 2) as updated_values
      FROM table
    """)
      .show()
  }
}
