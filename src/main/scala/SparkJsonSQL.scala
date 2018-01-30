import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.SparkSession

object SparkJsonSQL extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Address(street: String, city: String, state: String, zip: String)

  case class Customer(first_name: String, last_name: String, address: Address)

  def getAddress(address: String): Address = {
    val splitAddress = address.split(",")
    Address(splitAddress(0).replace("[", ""), splitAddress(1), splitAddress(2), splitAddress(3).replace("]", ""))
  }

  val spark = SparkSession.builder
    .appName("Spark Json")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", "checkpoint")
    .getOrCreate()

  import spark.implicits._

  val customersDS = spark.sqlContext.read.json("json_file_path")
    .map(element => Customer(element(1).toString, element(2).toString, getAddress(element(0).toString)))
    .cache()

  customersDS.show()

  customersDS.select(customersDS("first_name"), customersDS("address.street")).show()

  spark.stop()

}
