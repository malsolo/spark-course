package com.malsolo.spark.course.sundogsoftware.ds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{round, sum}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}

object TotalSpentByCustomerDataset {

  case class CustomerOrders(customerId: Int, itemId: Int, amountSpent: Double)

  def main(args: Array[String]): Unit = {
    Logger.getLogger(TotalSpentByCustomerDataset.getClass).setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("TotalSpentByCustomerDataset")
      .master("local[*]")
      .getOrCreate()

    val customerOrdersSchema = new StructType()
      .add("customerId", IntegerType, nullable = true)
      .add("itemId", IntegerType, nullable = true)
      .add("amountSpent", DoubleType, nullable = true)

    import spark.implicits._
    val customerDS = spark.read
      .schema(customerOrdersSchema)
      .csv("data/customer-orders.csv")
      .as[CustomerOrders]

    val totalByCustomersSorted = customerDS
      .groupBy("customerId")
      .agg(round(sum("amountSpent"), 2)
        .alias("total_spent"))
      .sort("total_spent")

    totalByCustomersSorted.show(totalByCustomersSorted.count.toInt)

  }

}
