import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkWordCountSQL extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Word(value: String)

  val sparkSession = SparkSession.builder()
    .appName("Spark Word Count SQL")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", "checkpoint")
    .getOrCreate()

  import sparkSession.implicits._

  val wordDS = sparkSession.sqlContext.read.textFile("input_file_path")
    .flatMap(_.split("\\W+"))
    .map(Word)

  val result = wordDS.groupBy("value").count.orderBy(desc("count")).cache()

  result.show()

  sparkSession.stop()

}
