import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingNetworkWordCount extends App {

  val (hostname, port) = if (args.length < 2) {
    ("localhost", "9999")
  } else {
    (args(0), args(1))
  }

  val sparkConf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("Spark Streaming Network Word Count")
  val ssc = new StreamingContext(sparkConf, Seconds(1))

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
