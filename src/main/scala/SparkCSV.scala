import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkCSV extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkSession = SparkSession.builder
    .appName("Spark CSV")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", "checkpoint")
    .getOrCreate()

  val babyNames = sparkSession.sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("csv_file_path")

  babyNames.createOrReplaceTempView("names")

  val distinctYears = sparkSession.sql("select distinct Year from names")

  distinctYears.show()

  sparkSession.stop()

}
