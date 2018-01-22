import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.sql.SparkSession

import scala.reflect.io.File

object SparkJsonSQL extends App {

  case class Address(street: String, city: String, state: String, zip: String)

  case class Customer(first_name: String, last_name: String, address: Address)

  case class Test(first_name: String, last_name: String)

  def getAddress(address: String): Address = {
    val splitAddress = address.split(",")
    Address(splitAddress(0).replace("[", ""), splitAddress(1), splitAddress(2), splitAddress(3).replace("]", ""))
  }

  if (File("config.properties").exists) {

    val input = new FileInputStream("config.properties")
    val properties = new Properties()

    properties.load(input)

    val spark = SparkSession.builder
      .appName("Spark Json")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val customersDS = spark.sqlContext.read.json(properties.getProperty("jsonFile"))
      .map(element => Customer(element(1).toString, element(2).toString, getAddress(element(0).toString)))
      .cache()

    customersDS.show()

    customersDS.select(customersDS("first_name"), customersDS("address.street")).show()

    spark.stop()

  } else {

    println("config.properties file not found.")

  }

}
