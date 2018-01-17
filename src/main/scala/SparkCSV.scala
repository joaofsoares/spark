import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.sql.SparkSession

import scala.reflect.io.File

object SparkCSV extends App {

  if (File("config.properties").exists) {

    val input = new FileInputStream("config.properties")
    val properties = new Properties()

    properties.load(input)

    val sparkSession = SparkSession.builder.
      master("local[2]")
      .appName("Spark CSV")
      .getOrCreate()

    val babyNames = sparkSession.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(properties.getProperty("csvFile"))

    babyNames.createOrReplaceTempView("names")

    val distinctYears = sparkSession.sqlContext.sql("select distinct Year from names")

    distinctYears.show

    sparkSession.stop()

  } else {

    println("config.properties file not found.")

  }

}
