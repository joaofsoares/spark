import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkCSVSQL extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Baby(year: String, firstName: String, county: String, sex: String, count: String)

  val sparkSession = SparkSession.builder
    .appName("Spark CSV")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", "checkpoint")
    .getOrCreate()

  import sparkSession.implicits._

  val babyNamesDS = sparkSession.sqlContext.read.csv("csv_file_path")
    .map(row => Baby(row(0).toString, row(1).toString, row(2).toString, row(3).toString, row(4).toString))
    .cache()

  babyNamesDS.select(babyNamesDS("year")).orderBy("year").distinct.show()

  // Save as text file in disk
  //  babyNamesDS.rdd.saveAsTextFile("output_file_path")

  sparkSession.stop()

}
