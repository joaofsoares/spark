import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
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

  val sparkStreaming = new StreamingContext(sparkConf, Seconds(10))

  val twitterStream = TwitterUtils.createStream(sparkStreaming, None)

  val eachLine = twitterStream.map(_.getText)

  val eachWord = eachLine.flatMap(_.split("\\s+"))

  val hashTags = eachWord.filter(_.startsWith("#"))

  val hashCount = hashTags.map((_, 1)).reduceByKey(_ + _).cache()

  val flippedHashCount = hashCount.transform(hash => hash.map(x => (x._2, x._1)))

  val sortedHashCount = flippedHashCount.transform(hash => hash.sortByKey(false))

  // this line isn't necessary. it was included just to print a formatted result.
  val resultHashTags = sortedHashCount.transform(hash => hash.map(x => (x._2, x._1)))

  // if you will, you can use sortedHashCount.print() instead of
  resultHashTags.print()

  sparkStreaming.start()
  sparkStreaming.awaitTermination()

}
