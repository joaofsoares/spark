import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkKafkaStreaming extends App {

  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("Spark Kafka Streaming")

  val ssc = new StreamingContext(conf, Seconds(10))

  val kafkaStream = KafkaUtils.createStream(ssc,
    "localhost:2181",
    "spark-streaming-consumer-group",
    Map("spark-topic" -> 5),
    StorageLevel.MEMORY_AND_DISK_SER)

  kafkaStream.print()

  ssc.start()
  ssc.awaitTermination()

}
