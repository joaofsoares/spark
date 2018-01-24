import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val (hostname, port) = if (args.length < 2) {
    ("localhost", "9999")
  } else {
    (args(0), args(1))
  }

  val conf = new SparkConf().setAppName("Spark Streaming").setMaster("local[2]")

  val sparkStreaming = new StreamingContext(conf, Seconds(10))

  val lines = sparkStreaming.socketTextStream(hostname, port.toInt)

  lines.flatMap(_.split("\\W+")).map((_, 1)).reduceByKey(_ + _).print()

  sparkStreaming.start()
  sparkStreaming.awaitTermination()

}
