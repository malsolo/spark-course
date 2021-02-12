package com.malsolo.spark.course.sundogsoftware.ds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkSqlDataset {

  case class Person(id: Int, name: String, age: Int, friends: Int)

  def main(args: Array[String]): Unit = {
    Logger.getLogger(SparkSqlDataset.getClass).setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("SparkSqlDataset")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val schemaPeople = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]

    schemaPeople.printSchema()

    schemaPeople.createOrReplaceTempView("people")

    val teenagers = spark.sql("SELECT * FROM people WHERE age >=13 and age <=19")

    val results = teenagers.collect()

    results.foreach(println)

    spark.stop()
  }

}
