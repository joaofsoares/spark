import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkJson extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkSession = SparkSession.builder
    .appName("Spark Json")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", "checkpoint")
    .getOrCreate()

  val customers = sparkSession.sqlContext.read.json("json_file_path")

  customers.createOrReplaceTempView("customers")

  customers.show()

  val firstNameCityState = sparkSession.sqlContext.sql("select first_name, address.city, address.state " +
    "from customers").cache()

  firstNameCityState.show()

  // Save as text file in disk
  //  firstNameCityState.rdd.saveAsTextFile("output_file_path")

  sparkSession.stop()

}
