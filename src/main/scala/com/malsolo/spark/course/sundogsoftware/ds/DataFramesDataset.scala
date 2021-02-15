package com.malsolo.spark.course.sundogsoftware.ds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DataFramesDataset {

  case class Person(id: Int, name: String, age: Int, friends: Int)

  def main(args: Array[String]): Unit = {
    Logger.getLogger(DataFramesDataset.getClass).setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val spark = SparkSession.builder.appName("DataFramesDataset")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val people = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]

    people.printSchema()

    println("SELECT NAME")
    people.select("name").show()

    println("SELECT * WHERE age < 21")
    people.filter(people("age") < 21).show()

    println("SELECT * WHERE GROUP BY age")
    people.groupBy("age").count().show()

    println("FUN with age")
    people.select(people("name"), people("age") + 10).show()

    spark.stop()
  }

}
