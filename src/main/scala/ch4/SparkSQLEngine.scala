package ch4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object SparkSQLEngine {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("SparkSQLEngine")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext

    val csvFile = "data/departuredelays.csv"
    val df = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(csvFile)

    df.createOrReplaceTempView("us_delay_flights_tbl")

    // Some queries
    spark
      .sql("""
      SELECT 
        distance
       ,origin
       ,destination
      FROM 
        us_delay_flights_tbl
      WHERE
        distance > 1000
      ORDER BY
        distance DESC
    """)
      .show(10)

    spark
      .sql("""
        SELECT
          date
         ,delay
         ,origin
         ,destination
        FROM
          us_delay_flights_tbl
        WHERE
          delay > 120         AND
          origin = 'SFO'      AND
          destination = 'ORD'
        ORDER BY 
          delay DESC
        """)
      .show(10)

    spark
      .sql("""
        SELECT 
          delay
         ,origin
         ,destination
         ,CASE
            WHEN delay > 360 THEN 'Very Long Delays'
            WHEN delay > 120 AND
                 delay < 360 THEN 'Long Delays'
            WHEN delay > 60  AND
                 delay < 120 THEN 'Short Delays'
            WHEN delay > 0   AND
                 delay < 60  THEN 'Tolerable Delays'
            WHEN delay = 0   THEN 'No Delays'
            ELSE                  'Early'
          END AS Flight_Delays
          FROM
            us_delay_flights_tbl 
          ORDER BY
            origin
           ,delay DESC
        """)
      .show(10)

    // Create a Database
    spark.sql("CREATE DATABASE IF NOT EXISTS learn_spark_db")
    spark.sql("USE learn_spark_db")

    // Create managed table into created database
    spark.sql("""
      CREATE TABLE IF NOT EXISTS managed_us_delay_flights_tbl (
        date STRING,
        delay INT,
        distance INT,
        origin STRING,
        destination STRING
      )
    """)
    // or we can do the same thing using the DataFrame API
    // val schema =
    //   "date STRING, delay INT, distance INT, origin STRING, destination STRING"
    // val flightsDf = spark.read.csv(csvFile, schema)
    // flightsDf.write.saveAsTable("managed_us_delay_flights_tbl")

    // Create unmanaged table
    spark.sql("""
      CREATE TABLE IF NOT EXISTS us_delay_flights_tbl(
        date STRING,
        delay INT,
        distance INT,
        origin STRING,
        destination STRING
        ) USING csv OPTIONS (
          PATH 'data/departuredelays.csv' 
        )
    """)
    // or within the DataFrame API
    // flights_df
    //  .write
    //  .option("path", "/tmp/data/us_flights_delay")
    //  .saveAsTable("us_delay_flights_tbl")

    // create view
    spark.sql("""
      CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_SFO_global_tmp_view AS
        SELECT 
          date, delay, origin, destination 
        FROM
          us_delay_flights_tbl
        WHERE
          origin = 'SFO'
    """)

    spark
      .sql("SELECT * FROM global_temp.us_origin_airport_SFO_global_tmp_view")
      .show(10)

    println("DATABASES")
    spark.catalog.listDatabases().show()

    println("TABLES")
    spark.catalog.listTables().show()

    println("COLUMNS of us_delay_flights_tbl")
    spark.catalog.listColumns("us_delay_flights_tbl").show()

    val usFlightsDF = spark.sql("SELECT * FROM us_delay_flights_tbl")
    val usFlightsDF2 = spark.table("us_delay_flights_tbl")
    usFlightsDF2.show(10)
  }
}
