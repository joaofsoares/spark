import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf().setAppName("Spark Streaming").setMaster("local[2]")

  val sparkStreaming = new StreamingContext(sparkConf, Seconds(1))

  val lines = sparkStreaming.socketTextStream("localhost", 9999)

  val eachWord = lines.flatMap(_.split("\\W+"))

  val wordCount = eachWord.map((_, 1))

  val totalWordCount = wordCount.reduceByKey(_ + _)

  // Print on console
  totalWordCount.print()

  // Save on disk creating a new folder for each bach
  totalWordCount.saveAsTextFiles("myLocalData/localData")

  // Save on disk creating just a folder and grouping each bach
  totalWordCount.foreachRDD((rdd, timestamp) => {
    println("Executing bach at: " + timestamp)
    rdd.cache()
    // Save as text file in disk
    //    rdd.saveAsTextFile("myLocalData/localData")
  })

  sparkStreaming.checkpoint("checkpoint")

  sparkStreaming.start()
  sparkStreaming.awaitTermination()

}
