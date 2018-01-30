import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkCSVSQL extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Baby(year: String, firstName: String, county: String, sex: String, count: String)

  val spark = SparkSession.builder
    .appName("Spark CSV")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", "checkpoint")
    .getOrCreate()

  val babyNamesDS = spark.sqlContext.read.csv("csv_file_path")
    .map(row => Baby(row(0).toString, row(1).toString, row(2).toString, row(3).toString, row(4).toString))
    .cache()

  babyNamesDS.select(babyNamesDS("year")).orderBy("year").distinct.show()

  spark.stop()

}
