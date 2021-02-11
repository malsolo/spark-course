package com.malsolo.spark.course.sundogsoftware.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import scala.math.min

object MinTemperatures {

  def main(args: Array[String]): Unit = {
    Logger.getLogger(MinTemperatures.getClass).setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MinTemperatures")

    val minTempsByStationId = sc.textFile("data/1800.csv")
      .map(parseline)
      .filter(ts => ts._2 == "TMIN")
      .map(ts => (ts._1, ts._3))
      .reduceByKey((t1, t2) => min(t1, t2))
      .collect()

    for (temps <- minTempsByStationId.sorted) {
      val stationId = temps._1
      val temp = temps._2
      val formattedTemp = f"$temp%.2f F"
      println(s"$stationId minimum temperature: $formattedTemp")
    }
  }

  def parseline(line: String): (String, String, Float) = {
    val fields = line.split(",")
    val stationId = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationId, entryType, temperature)
  }

}
