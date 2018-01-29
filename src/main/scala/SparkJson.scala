import java.io.FileInputStream
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.reflect.io.File

object SparkJson extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  if (File("config.properties").exists) {

    val input = new FileInputStream("config.properties")
    val properties = new Properties()

    properties.load(input)

    val sparkSession = SparkSession.builder
      .appName("Spark Json")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", "checkpoint")
      .getOrCreate()

    val customers = sparkSession.sqlContext.read.json(properties.getProperty("jsonFile"))

    customers.createOrReplaceTempView("customers")

    customers.show()

    val firstNameCityState = sparkSession.sqlContext.sql("select first_name, address.city, address.state " +
      "from customers")

    firstNameCityState.show()

    sparkSession.stop()

  } else {

    println("config.properties file not found.")

  }

}
