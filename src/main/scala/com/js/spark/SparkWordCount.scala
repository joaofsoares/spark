package com.js.spark

import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.io.File

object SparkWordCount extends App {

  val InputFile = "in.txt"
  val OutputDir = "out"

  println(
    if (File(OutputDir).deleteRecursively()) {
      "Loading... cleaning directory... ready."
    } else {
      "Loading... ready."
  })

  val sparkConf = new SparkConf().setAppName("Spark Word Count").setMaster("local")
  val sc = new SparkContext(sparkConf)

  sc.textFile("in.txt")
    .flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)
    .saveAsTextFile("out")

  sc.stop()

}
