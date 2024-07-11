package ch3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object DatasetAPI {

  case class DeviceIoTData(
      battery_level: Long,
      c02_level: Long,
      cca2: String,
      cca3: String,
      cn: String,
      device_id: Long,
      device_name: String,
      humidity: Long,
      ip: String,
      latitude: Double,
      lcd: String,
      longitude: Double,
      scale: String,
      temp: Long,
      timestamp: Long
  )

  case class DeviceTempByCountry(
      temp: Long,
      device_name: String,
      device_id: Long,
      cca3: String
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("DatasetAPI")
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    val row = Row(350, true, "Learning spark 2E", null)
    println(row.get(1))

    val ds = spark.read
      .json("data/iot_devices.json")
      .as[DeviceIoTData]

    ds.show(5, false)

    val filterTempDS = ds.filter({ d => { d.temp > 30 && d.humidity > 70 } })
    filterTempDS.show(5, false)

    val dsTemp = ds
      .filter(d => { d.temp > 25 })
      .map(d => (d.temp, d.device_name, d.device_id, d.cca3))
      .toDF("temp", "device_name", "device_id", "cca3")
      .as[DeviceTempByCountry]

    dsTemp.show(5, false)

    val dsTemp2 = ds
      .select($"temp", $"device_name", $"device_id", $"cca3")
      .where("temp > 25")
      .as[DeviceTempByCountry]

    dsTemp2.show(5, false)

    // Some examples
    // 1. Detect failing devices with battery levels below a threshold.
    // 2. Identify offending countries with high levels of CO2 emissions.
    // 3. Compute the min and max values for temperature, battery level, CO2, and
    // humidity.
    // 4. Sort and group by average temperature, CO2, humidity, and country.
  }
}
