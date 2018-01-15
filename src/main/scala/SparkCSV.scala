import org.apache.spark.sql.SparkSession

object SparkCSV extends App {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("Spark CSV")
    .getOrCreate()

  val sqlContext = sparkSession.sqlContext

  val babyNames = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/baby_names_2007.csv")

  babyNames.createOrReplaceTempView("names")

  val distinctYears = sqlContext.sql("select distinct Year from names")

  distinctYears.show

}
