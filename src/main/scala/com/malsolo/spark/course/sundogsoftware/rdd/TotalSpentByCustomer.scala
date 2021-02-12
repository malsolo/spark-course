package com.malsolo.spark.course.sundogsoftware.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object TotalSpentByCustomer {

  def extractCustomerPricePairs(line: String): (Int, Float) = {
    val fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger(TotalSpentByCustomer.getClass).setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "TotalSpentByCustomer")

    val results = sc.textFile("data/customer-orders.csv")
      .map(extractCustomerPricePairs)
      .reduceByKey(_ + _)
      .map(_.swap)
      .sortByKey()
      .collect()

    results.foreach(println)

  }

}
