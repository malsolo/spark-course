package com.malsolo.spark.course.sundogsoftware.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext


object FriendsByAge {

  def main(args: Array[String]): Unit = {
    Logger.getLogger(FriendsByAge.getClass).setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FriendsByAge")

    val averagesByAge = sc.textFile("data/fakefriends-noheader.csv")
      .map(parseLine)
      .mapValues(num => (num, 1)) //Tuple with number of friends for an age AND number of people with that age
      .reduceByKey((t1, t2) => (t1._1 + t2._1, t1._2 + t2._2))
      .mapValues(tuple => tuple._1 / tuple._2)
      .collect()

    averagesByAge.sorted.foreach(println)
  }

  def parseLine(line: String): (Int, Int) = {
    val fields = line.split(",")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    (age, numFriends)
  }

}
