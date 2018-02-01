import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkContStructStreaming extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkSession = SparkSession.builder
    .appName("Spark Cont Struct Streaming")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", "checkpoint")
    .getOrCreate()

  val sparkConnection = sparkSession.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

  import sparkSession.implicits._

  val input = sparkConnection.as[String] // TODO more effective examples

  // TODO if the previous task is incremented probably append most be complete
  val output = input.writeStream.outputMode("append").format("console").start()

  output.awaitTermination()

}
