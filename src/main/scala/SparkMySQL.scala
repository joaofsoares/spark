import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.sql.SparkSession

object SparkMySQL extends App {

  val input = new FileInputStream("config.properties")
  val properties = new Properties()

  properties.load(input)

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("Spark MySQL")
    .getOrCreate()

  val dataframeMySQL = sparkSession.read.format("jdbc")
    .option("url", "jdbc:mysql://localhost/" + properties.getProperty("database"))
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "users")
    .option("user", properties.getProperty("username"))
    .option("password", properties.getProperty("password"))
    .load()

  input.close()

  dataframeMySQL.show

  dataframeMySQL.createOrReplaceTempView("users")

  dataframeMySQL.sqlContext.sql("select * from users").collect.foreach(println)

  sparkSession.stop()

}
