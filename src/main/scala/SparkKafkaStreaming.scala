import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkKafkaStreaming extends App {

  val ssc = new StreamingContext("local[2]", "Spark Kafka Streaming", Seconds(10))

  val kafkaStream = KafkaUtils.createStream(ssc,
    "localhost:2181",
    "spark-streaming-consumer-group",
    Map("spark-topic" -> 1))

  // print input
  // kafkaStream.map(_._2).print()

  kafkaStream
    .map(_._2)
    .flatMap(_.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)
    .print()

  ssc.start()
  ssc.awaitTermination()

}
