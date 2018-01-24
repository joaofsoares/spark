import java.io.FileInputStream
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.reflect.io.File

object SparkCSV extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  if (File("config.properties").exists) {

    val input = new FileInputStream("config.properties")
    val properties = new Properties()

    properties.load(input)

    val spark = SparkSession.builder
      .appName("Spark CSV")
      .getOrCreate()

    val babyNames = spark.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(properties.getProperty("csvFile"))

    babyNames.createOrReplaceTempView("names")

    val distinctYears = spark.sql("select distinct Year from names")

    distinctYears.show

    spark.stop()

  } else {

    println("config.properties file not found.")

  }

}
