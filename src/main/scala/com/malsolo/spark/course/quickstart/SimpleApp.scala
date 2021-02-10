package com.malsolo.spark.course.quickstart

import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD Quickstart").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val fileName = "spark_readme.md"
    val textFile = sc.textFile(fileName)
    val wordCount = textFile.flatMap(line => line.split(" "))
      .groupBy(identity).count()
    println(s"word count?: $wordCount")
    val count = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .collect()
    println(s"words count: ${count.mkString("Array(", ", ", ")")}")
  }

}
