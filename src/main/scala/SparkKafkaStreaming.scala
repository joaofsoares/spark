import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkKafkaStreaming extends App {

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("Spark Kafka Streaming")

  val ssc = new StreamingContext(conf, Seconds(10))

  val kafkaStream = KafkaUtils.createStream(ssc,
    "localhost:2181",
    "spark-streaming-consumer-group",
    Map("spark-topic" -> 1))

  kafkaStream
    .map(_._2)
    .flatMap(_.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)
    .print()

  ssc.start()
  ssc.awaitTermination()

}
