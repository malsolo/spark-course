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
      .reduceByKey( (a, b) => a + b )
      .map( t => (t._2, t._1) )
      .sortByKey()

    for (wordCount <- wordCountsSorted) {
      val count = wordCount._1
      val word = wordCount._2
      println(s"$word: $count")
    }

  }

}
