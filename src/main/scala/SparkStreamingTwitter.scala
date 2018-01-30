import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils

import scala.io.Source

object SparkStreamingTwitter extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Word(value: String)

  val prefix = "twitter4j.oauth."
  Source.fromFile("data/twitter-settings.txt").getLines().foreach(line => {
    val splitLine = line.split(" ")
    System.setProperty(prefix + splitLine(0), splitLine(1))
  })

  val sparkConf = new SparkConf().setAppName("Spark Streaming Twitter").setMaster("local[*]")

  val sparkStreaming = new StreamingContext(sparkConf, Seconds(1))

  val twitterStream = TwitterUtils.createStream(sparkStreaming, None)

  val eachLine = twitterStream.map(_.getText)

  val eachWord = eachLine.flatMap(_.split("\\s+"))

  val hashTags = eachWord.filter(_.startsWith("#"))

  val hashTagsKeyValue = hashTags.map((_, 1))

  val hashCount = hashTagsKeyValue.reduceByKeyAndWindow((x, y) => x + y, (x, y) => x - y, Seconds(300), Seconds(1)).cache()

  val sortedHashCount = hashCount.transform(rdd => rdd.sortBy(x => x._2, ascending = false))

  sortedHashCount.print()

  sparkStreaming.checkpoint("checkpoint")
  sparkStreaming.start()
  sparkStreaming.awaitTermination()

}
