import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkStructuredStreaming extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkSession = SparkSession.builder
    .appName("Spark Streaming Structured")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", "checkpoint")
    .getOrCreate()

  import sparkSession.implicits._

  // reading every file from a directory
  //  val lines = spark.readStream.text("logs")
  val lines = sparkSession.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

  val wordCounts = lines.as[String].flatMap(_.split("\\W+")).groupBy("value").count()

  val process = wordCounts.writeStream.outputMode("complete").format("console").start()

  process.awaitTermination()

}
