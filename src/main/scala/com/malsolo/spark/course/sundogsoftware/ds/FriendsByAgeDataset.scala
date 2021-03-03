package com.malsolo.spark.course.sundogsoftware.ds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, round}

object FriendsByAgeDataset {

  case class FakeFriends(id: Int, name: String, age: Int, friends: Long)

  def main(args: Array[String]): Unit = {
    Logger.getLogger(FriendsByAgeDataset.getClass).setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("FriendsByAgeDataset")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val ds = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[FakeFriends]

    val friendsByAge = ds.select("age", "friends")

    friendsByAge.groupBy("age").avg("friends").show()

    friendsByAge.groupBy("age").avg("friends").orderBy("age").show()

    friendsByAge.groupBy("age").agg(round(avg("friends"), 2)).orderBy("age").show()

    friendsByAge.groupBy("age").agg(round(avg("friends"), 2).alias("friends_avg")).orderBy("age").show()

  }

}
