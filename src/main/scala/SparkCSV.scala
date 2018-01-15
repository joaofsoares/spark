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
      master("local")
      .appName("Spark CSV")
      .getOrCreate()

    val sqlContext = sparkSession.sqlContext

    val babyNames = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(properties.getProperty("csvFile"))

    babyNames.createOrReplaceTempView("names")

    val distinctYears = sqlContext.sql("select distinct Year from names")

    distinctYears.show

    sparkSession.stop()

  } else {

    println("config.properties file not found.")

  }

}
