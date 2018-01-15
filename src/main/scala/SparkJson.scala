import org.apache.spark.sql.SparkSession

object SparkJson extends App {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("Spark Json")
    .getOrCreate()

  val sqlContext = sparkSession.sqlContext

  val customers = sqlContext.read.json("data/customers.json")

  customers.createOrReplaceTempView("customers")

  customers.show

  val firstNameCityState = sqlContext.sql("select first_name, address.city, address.state from customers")

  firstNameCityState.show

  sparkSession.stop()

}
