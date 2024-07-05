package ch3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object CommonDs {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Struct")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    // val sampleDF = spark.read
    //   .option("samplingRatio", 0.001)
    //   .option("header", true)
    //   .csv("data/sf-fire-calls.csv")

    val fireSchema = StructType(
      Array(
        StructField("CallNumber", IntegerType, true),
        StructField("UnitID", StringType, true),
        StructField("IncidentNumber", IntegerType, true),
        StructField("CallType", StringType, true),
        StructField("CallDate", StringType, true),
        StructField("WatchDate", StringType, true),
        StructField("CallFinalDisposition", StringType, true),
        StructField("AvailableDtTm", StringType, true),
        StructField("Address", StringType, true),
        StructField("City", StringType, true),
        StructField("Zipcode", IntegerType, true),
        StructField("Battalion", StringType, true),
        StructField("StationArea", StringType, true),
        StructField("Box", StringType, true),
        StructField("OriginalPriority", StringType, true),
        StructField("Priority", StringType, true),
        StructField("FinalPriority", IntegerType, true),
        StructField("ALSUnit", BooleanType, true),
        StructField("CallTypeGroup", StringType, true),
        StructField("NumAlarms", IntegerType, true),
        StructField("UnitType", StringType, true),
        StructField("UnitSequenceInCallDispatch", IntegerType, true),
        StructField("FirePreventionDistrict", StringType, true),
        StructField("SupervisorDistrict", StringType, true),
        StructField("Neighborhood", StringType, true),
        StructField("Location", StringType, true),
        StructField("RowID", StringType, true),
        StructField("Delay", FloatType, true)
      )
    )

    val fireDF = spark.read
      .schema(fireSchema)
      .option("option", "true")
      .csv("data/sf-fire-calls.csv")

    // fireDF.write.format("parquet").save("data/fire_output_as_parquet")

    val fewFireDF = fireDF
      .select("IncidentNumber", "AvailableDtTm", "CallType")
      .where($"CallType" =!= "Medical Incident")

    fewFireDF.show(5, true)

    fireDF
      .select("CallType")
      .where($"CallType".isNotNull)
      .agg(countDistinct('CallType) as 'DistinctCallTypes)
      .show()

    fireDF
      .select("CallType")
      .where($"CallType".isNotNull)
      .distinct()
      .show(10, false)

    val newFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedinMins")
    newFireDF
      .select("ResponseDelayedinMins")
      .where($"ResponseDelayedinMins" > 5)
      .show(5, false)

    val fireTsDF = newFireDF
      .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
      .drop("CallDate")
      .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
      .drop("WatchDate")
      .withColumn(
        "AvailableDtTs",
        to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a")
      )
      .drop("AvilableDtTs")

    fireTsDF
      .select("IncidentDate", "OnWatchDate", "AvailableDtTs")
      .show(5, false)

    fireTsDF
      .select(year($"IncidentDate"))
      .distinct()
      .orderBy(year($"IncidentDate"))
      .show()

    import org.apache.spark.sql.{functions => F}
    fireTsDF
      .select(
        F.sum("NumAlarms"),
        F.avg("ResponseDelayedinMins"),
        F.min("ResponseDelayedinMins"),
        F.max("ResponseDelayedinMins")
      )
      .show()

    /* Some examples
      • What were all the different types of fire calls in 2018?
      • What months within the year 2018 saw the highest number of fire calls?
      • Which neighborhood in San Francisco generated the most fire calls in 2018?
      • Which neighborhoods had the worst response times to fire calls in 2018?
      • Which week in the year in 2018 had the most fire calls?
      • Is there a correlation between neighborhood, zip code, and number of fire calls?
      • How can we use Parquet files or SQL tables to store this data and read it back?
     */

    spark.stop()
  }
}
