import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkKafkaStreaming extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf().setAppName("Spark Kafka Streaming").setMaster("local[2]")
  val ssc = new StreamingContext(sparkConf, Seconds(1))

  // insert kafka broker address
  val kafkaParameters = Map("metadata.broker.list" -> "localhost:9092")

  // insert all topic here
  val topics = List("spark-topic").toSet

  val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParameters, topics)

  val input = kafkaStream.map(_._2)

  val eachWord = input.flatMap(_.split("\\W+"))

  val wordCount = eachWord.map((_, 1))

  val totalWordCount = wordCount.reduceByKey(_ + _)

  val sortedWordCount = totalWordCount.transform(rdd => rdd.sortBy(_._2, ascending = false)).cache()

  sortedWordCount.print()

  ssc.checkpoint("checkpoint")

  // Save as text file in disk
  //  sortedWordCount.foreachRDD((rdd, timestamp) => {
  //    println("Saving " + rdd + " at " + timestamp)
  //    rdd.saveAsTextFile("output_file_path")
  //  })

  ssc.start()
  ssc.awaitTermination()

}
