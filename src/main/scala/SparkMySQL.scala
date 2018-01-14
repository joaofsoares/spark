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

  val userData = sparkSession.read.format("jdbc")
    .option("url", "jdbc:mysql://localhost/" + properties.getProperty("database"))
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "users")
    .option("user", properties.getProperty("username"))
    .option("password", properties.getProperty("password"))
    .load()

  val departmentData = sparkSession.read.format("jdbc")
    .option("url", "jdbc:mysql://localhost/" + properties.getProperty("database"))
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "department")
    .option("user", properties.getProperty("username"))
    .option("password", properties.getProperty("password"))
    .load()

  input.close()

  // Single Table

  userData.show

  userData.createOrReplaceTempView("users")

  userData.select("id").show

  userData.filter(userData("username").endsWith("1")).show

  // Join - Multi Table

  departmentData.show

  departmentData.createOrReplaceTempView("department")

  val joinTable = sparkSession.sqlContext.sql("select u.id, u.username, d.label from users u join department d on u.id = d.id ")

  joinTable.filter(joinTable("username").endsWith("1")).show

  joinTable.show

  sparkSession.stop()

}
