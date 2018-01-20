import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingNetworkWordCount extends App {

  val (hostname, port) = if (args.length < 2) {
    ("localhost", "9999")
  } else {
    (args(0), args(1))
  }
  val ssc = new StreamingContext("local[2]", "Spark Streaming Network Word Count", Seconds(1))

  //  val lines = ssc.socketTextStream("localhost", 9999)
  val lines = ssc.socketTextStream(hostname, port.toInt, StorageLevel.MEMORY_AND_DISK_SER)

  lines
    .flatMap(_.split(" "))
    .map(x => (x, 1))
    .reduceByKey(_ + _)
    .print()

  ssc.start()
  ssc.awaitTermination()

}
