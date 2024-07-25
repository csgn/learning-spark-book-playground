package ch3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object Struct {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Struct")
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    // ####################################################
    val data = List(
      ("Brooke", 20),
      ("Denny", 31),
      ("Jules", 30),
      ("TD", 35),
      ("Brooke", 25)
    )
    val dataRDD = sc.parallelize(data)
    println("RDD", dataRDD)

    val agesRDD =
      dataRDD
        /*
         * ("Brooke", (20, 1))
         * ("Denny",  (31, 1))
         * ("Jules",  (30, 1))
         * ("TD",     (35, 1))
         * ("Brooke", (25, 1))
         * */
        .map(x => (x._1, (x._2, 1)))
        /*
         * ("Brooke", (20, 1))
         * ("Brooke", (25, 1))
         *
         * x = (20, 1)
         * y = (25, 1)
         * (20+25, 2)
         * */
        .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
        /*
         * x = ("Brooke", (45, 2))
         * ("Brooke", 45 / 2)
         * */
        .map(x => (x._1, x._2._1 / x._2._2))

    val dataDF = spark.createDataFrame(data).toDF("name", "age")
    val agesDF = dataDF
      .groupBy("name")
      .agg(avg("age"))

    agesDF.show()

    // ####################################################
    // Define Schemas
    // with programatically
    val schema = StructType(
      Array(
        StructField("author", StringType, false),
        StructField("title", StringType, false),
        StructField("pages", IntegerType, false)
      )
    )
    // or we can define with DDL which is much simpler and easier to read
    // val ddlSchema = "author STRING, title STRING, pages INT"
    // val data2 = Seq(
    //   ("author1", "title1", 1),
    //   ("author2", "title2", 2),
    //   ("author3", "title3", 3)
    // )
    //
    // val ddlDF = data2.toDF(ddlSchema)
    // ddlDF.show()

    // ####################################################
    // ingest json
    // path data/blogs.json
    val jsonFile = "data/blogs.json"
    val jsonSchema = StructType(
      Array(
        StructField("id", IntegerType, false),
        StructField("first", StringType, false),
        StructField("last", StringType, false),
        StructField("url", StringType, false),
        StructField("published", StringType, false),
        StructField("hits", IntegerType, false),
        StructField("campaigns", ArrayType(StringType), false)
      )
    )
    val blogsDF = spark.read.schema(jsonSchema).json(jsonFile)
    // show Dataframe schema as output
    blogsDF.show(false)

    // print schema
    println(blogsDF.printSchema())
    println(blogsDF.schema)

    // ####################################################
    // Columns and Expressions
    println(blogsDF.columns.toList)
    println(blogsDF.col("id"))

    blogsDF.select(expr("hits * 2").as("doubleHits")).show(2)
    blogsDF.select((col("hits") * 2).as("doubleHits")).show(2)

    blogsDF.withColumn("Big Hitters", (expr("hits > 10000"))).show()
    blogsDF.withColumn("Big Hitters", col("hits") > 10000).show()

    blogsDF
      .withColumn(
        "AuthorsId",
        (concat(expr("first"), expr("last"), expr("id")))
      )
      .select(col("AuthorsId"))
      .show(4)

    blogsDF.select("hits").show(2)

    blogsDF.sort(col("id").desc).show()
    blogsDF.sort($"id".desc).show()

    // ####################################################
    // Rows
    val blogRow = Row(
      6,
      "Reynold",
      "Xin",
      "https://tinyurl.6",
      255568,
      "3/2/2015",
      Array("twitter", "LinkedIn")
    )
    println(blogRow(0), blogRow(1))

    val rows = Seq(("Matei Zaharia", "CA"), ("Reynold Xin", "CA"))
    val rowsDF = rows.toDF("Author", "State")
    rowsDF.show()

    spark.stop()
  }
}
