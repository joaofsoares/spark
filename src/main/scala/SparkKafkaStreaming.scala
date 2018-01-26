import org.apache.log4j.{ Level, Logger }
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{ Seconds, StreamingContext }

object SparkKafkaStreaming extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf().setAppName("Spark Kafka Streaming").setMaster("local[2]")
  val ssc = new StreamingContext(sparkConf, Seconds(10))

  val kafkaStream = KafkaUtils.createStream(
    ssc,
    "localhost:2181",
    "spark-streaming-consumer-group",
    Map("spark-topic" -> 1))

  // print input
  // kafkaStream.map(_._2).print()

  kafkaStream
    .map(_._2)
    .flatMap(_.split("\\W+"))
    .map((_, 1))
    .reduceByKey(_ + _)
    .print()

  ssc.start()
  ssc.awaitTermination()

}
