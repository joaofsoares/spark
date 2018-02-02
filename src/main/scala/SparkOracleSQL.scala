import java.io.FileInputStream
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.reflect.io.File

object SparkOracleSQL extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  if (File("config.properties").exists) {

    val input = new FileInputStream("config.properties")
    val properties = new Properties()

    properties.load(input)

    val sparkSession = SparkSession.builder
      .appName("Spark Oracle SQL")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", "checkpoint")
      .getOrCreate()

    val citiesData = sparkSession.sqlContext.read.format("jdbc")
      .option("url", "jdbc:oracle:thin:" + properties.getProperty("oracleConnection"))
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("dbtable", "population_data")
      .option("user", properties.getProperty("oracleConnectionUsername"))
      .option("password", properties.getProperty("oracleConnectionPassword"))
      .load().cache()

    citiesData.show()

    citiesData.select("stfid").show()

    // Save as text file in disk
    //    citiesData.rdd.saveAsTextFile("output_file_path")

    sparkSession.stop()

  } else {

    println("config.properties file not found.")

  }

}
