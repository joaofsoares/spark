import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingNetworkWordCount extends App {

  if (args.length < 2) {
    System.err.println("Usage: Spark Streaming Network Word Count <hostname> <port>")
    System.exit(1)
  }

  val sparkConf = new SparkConf().setMaster("local").setAppName("Spark Streaming Network Word Count")
  val ssc = new StreamingContext(sparkConf, Seconds(1))

  val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)

  val words = lines.flatMap(_.split(" "))
  val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

  wordCounts.print()

  ssc.start()
  ssc.awaitTermination()

}