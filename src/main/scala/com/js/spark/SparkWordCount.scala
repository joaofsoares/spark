package com.js.spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount extends App {

  val sparkConf = new SparkConf().setAppName("Spark Word Count").setMaster("local")

  val sc = new SparkContext(sparkConf)

  val inputFile = sc.textFile("in.txt")

  val count = inputFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

  count.saveAsTextFile("out")

  sc.stop()

}
