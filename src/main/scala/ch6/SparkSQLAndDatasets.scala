package ch6

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import scala.util.Random._

case class Bloggers(
    id: BigInt,
    first: String,
    last: String,
    url: String,
    published: String,
    hits: String,
    campaigns: Array[String]
)

case class Usage(uid: BigInt, uname: String, usage: Int)
case class UsageCost(uid: BigInt, uname: String, usage: Int, cost: Double)

case class Person(
    id: Integer,
    firstName: String,
    middleName: String,
    lastName: String,
    gender: String,
    birthDate: String,
    ssn: String,
    salary: String
)

object SparkSQLAndDatasets {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("SparkSQLAndDatasets")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext

    val bloggers = "data/blogs.json"
    val bloggersDS = spark.read
      .format("json")
      .option("path", bloggers)
      .load()
      .as[Bloggers]

    val r = new scala.util.Random(42)
    val data =
      for (i <- 0 to 1000)
        yield (Usage(
          i,
          "user-" + r.alphanumeric.take(5).mkString(""),
          r.nextInt(1000)
        ))

    val dsUsage = spark.createDataset(data)
    dsUsage.show(10)

    dsUsage
      .filter(d => d.usage > 900)
      .orderBy(desc("usage"))
      .show(5, false)

    def filterWithUsage(u: Usage) = u.usage > 900
    dsUsage.filter(filterWithUsage(_)).orderBy(desc("usage")).show(5, false)

    dsUsage
      .map(u => {
        if (u.usage > 750) u.usage * .15
        else u.usage * .50
      })
      .show(5, false)

    def computeCostUsage(usage: Int): Double = {
      if (usage > 750) usage * .15
      else usage * .50
    }
    dsUsage.map(u => { computeCostUsage(u.usage) }).show(5, false)

    def computeUserCostUsage(u: Usage): UsageCost = {
      val v = if (u.usage > 750) u.usage * .15 else u.usage * .50
      UsageCost(u.uid, u.uname, u.usage, v)
    }

    dsUsage.map(u => { computeUserCostUsage(u) }).show(5)
  }
}
