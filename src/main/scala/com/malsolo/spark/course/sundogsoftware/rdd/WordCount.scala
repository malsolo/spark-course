package com.malsolo.spark.course.sundogsoftware.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCount {

  def main(args: Array[String]): Unit = {
    Logger.getLogger(WordCount.getClass).setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val sc = new SparkContext("local", "WordCount") //Watch out! if you use local[*] the order could change because the results are processed in several nodes

    val wordCountsSorted = sc.textFile("data/book.txt")
      .flatMap(x => x.split("\\W+"))
      .map(word => word.toLowerCase())
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map({ case (x, y) => (y, x)} )
      .sortByKey(ascending = false)
      .map( t => t.swap )
      .take(10)

    for (wordCount <- wordCountsSorted) {
      val word = wordCount._1
      val count = wordCount._2
      println(s"$word: $count")
    }

  }

}
