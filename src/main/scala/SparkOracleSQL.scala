import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.sql.SparkSession

import scala.reflect.io.File

object SparkOracleSQL extends App {

  if (File("config.properties").exists) {

    val input = new FileInputStream("config.properties")
    val properties = new Properties()

    properties.load(input)

    val sparkSession = SparkSession.builder.
      master("local[2]")
      .appName("Spark Oracle SQL")
      .getOrCreate()

    val citiesData = sparkSession.sqlContext.read.format("jdbc")
      .option("url", "jdbc:oracle:thin:" + properties.getProperty("oracleConnection"))
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("dbtable", "population_data")
      .option("user", properties.getProperty("oracleConnectionUsername"))
      .option("password", properties.getProperty("oracleConnectionPassword"))
      .load()

    citiesData.show()

    citiesData.select("stfid").show()

    sparkSession.stop()

  } else {

    println("config.properties file not found.")

  }

}
