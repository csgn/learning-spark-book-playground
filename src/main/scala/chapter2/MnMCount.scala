package chapter2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MnMCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("MnMCount")
      .getOrCreate()

    if (args.length < 1) {
      print("Usage: MnMcount <mnm_file_dataset>")
      sys.exit(1)
    }

    // get the M&M data set file name
    val mnmFile = args(0)

    // read the file into a Spark DataFrame
    val mnmDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(mnmFile)

    // display DataFrame
    mnmDF.show(5, false)

    // aggregate count of all colors and groupBy state and color
    // orderBy descending order
    val countMnMDF = mnmDF
      .select("State", "Color", "Count")
      .groupBy("State", "Color")
      .sum("Count")
      .orderBy(desc("sum(Count)"))

    // Show the resulting aggregations for all the states and colors
    countMnMDF.show(60)
    println(s"Total Rows = ${countMnMDF.count()}")

    println()

    // Find the aggregate counts for California by filtering
    val caCountMnMDF = mnmDF
      .select("State", "Color", "Count")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))
    // Show the resulting aggregations for California
    caCountMnMDF.show(10)
    // Stop the SparkSession
    spark.stop()
  }

}
