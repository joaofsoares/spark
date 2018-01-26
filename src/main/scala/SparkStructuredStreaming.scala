import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.SparkSession

object SparkStructuredStreaming extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val (hostname, port) = if (args.length < 2) {
    ("localhost", "9999")
  } else {
    (args(0), args(1))
  }

  val spark = SparkSession.builder.appName("Spark Streaming Structured").master("local[*]").getOrCreate()

  val lines = spark.readStream.format("socket").option("host", hostname).option("port", port.toInt).load()

  import spark.implicits._

  val wordCounts = lines.as[String].flatMap(_.split("\\W+")).groupBy("value").count()

  val process = wordCounts.writeStream.outputMode("complete").format("console").start()

  process.awaitTermination()

}
