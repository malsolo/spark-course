package com.malsolo.spark.course.sundogsoftware.ds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.round
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}

object MinTemperaturesDataset {

  case class Temperature(stationID: String, date: Int, measureType: String, temperature: Float)

  def main(args: Array[String]): Unit = {
    Logger.getLogger(MinTemperaturesDataset.getClass).setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("MinTemperaturesDataset")
      .master("local[*]")
      .getOrCreate()

    val temperatureSchema = new StructType()
      .add("stationID", StringType, nullable = true)
      .add("date", IntegerType, nullable = true)
      .add("measureType", StringType, nullable = true)
      .add("temperature", FloatType, nullable = true)

    import spark.implicits._
    val ds = spark.read
      .schema(temperatureSchema)
      .csv("data/1800.csv")
      .as[Temperature]

    val minTempsByStationF = ds
      .filter($"measureType" === "TMIN")
      .select("stationID", "temperature")
      .groupBy("stationID")
      .min("temperature")
      .withColumn("temperature", round($"min(temperature)" * 0.1f * (9.0f / 5.0f) + 32.0f, 2))
      .select("stationID", "temperature")
      .sort("temperature")

    val results = minTempsByStationF.collect()

    for (result <- results) {
      val station = result(0)
      val temp = result(1).asInstanceOf[Float]
      val formattedTemp = f"$temp%.2f F"

      println(s"$station minimum temperature: $formattedTemp")
    }

  }

}
