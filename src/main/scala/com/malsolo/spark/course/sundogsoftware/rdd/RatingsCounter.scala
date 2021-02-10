package com.malsolo.spark.course.sundogsoftware.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object RatingsCounter {

  def main(args: Array[String]): Unit = {
    Logger.getLogger(RatingsCounter.getClass).setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "RatingsCounter")

    val sortedResults = sc.textFile("data/ml-100k/u.data")
      .map(line => line.split("\t")(2))
      .countByValue()
      .toSeq.sortBy(_._1)

    sortedResults.foreach(println)

  }

}
