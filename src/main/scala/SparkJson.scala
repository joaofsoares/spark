import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.sql.SparkSession

import scala.reflect.io.File

object SparkJson extends App {

  if (File("config.properties").exists) {

    val input = new FileInputStream("config.properties")
    val properties = new Properties()

    properties.load(input)

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("Spark Json")
      .getOrCreate()

    val sqlContext = sparkSession.sqlContext

    val customers = sqlContext.read.json(properties.getProperty("jsonFile"))

    customers.createOrReplaceTempView("customers")

    customers.show

    val firstNameCityState = sqlContext.sql("select first_name, address.city, address.state from customers")

    firstNameCityState.show

    sparkSession.stop()

  } else {

    println("config.properties file not found.")

  }

}
