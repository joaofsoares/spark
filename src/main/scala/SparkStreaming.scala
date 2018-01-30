import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf().setAppName("Spark Streaming").setMaster("local[2]")

  val sparkStreaming = new StreamingContext(conf, Seconds(1))

  val lines = sparkStreaming.socketTextStream("localhost", 9999)

  val eachWord = lines.flatMap(_.split("\\W+"))

  val wordCount = eachWord.map((_, 1))

  val totalWordCount = wordCount.reduceByKey(_ + _)

  totalWordCount.print()

  sparkStreaming.checkpoint("checkpoint")

  sparkStreaming.start()
  sparkStreaming.awaitTermination()

}
