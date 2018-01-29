import java.io.FileInputStream
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.reflect.io.File

object SparkMySQL extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  if (File("config.properties").exists) {

    val input = new FileInputStream("config.properties")
    val properties = new Properties()

    properties.load(input)

    val sparkSession = SparkSession.builder
      .appName("Spark MySQL")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", "checkpoint")
      .getOrCreate()

    val userData = sparkSession.sqlContext.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost/" + properties.getProperty("database"))
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "users")
      .option("user", properties.getProperty("username"))
      .option("password", properties.getProperty("password"))
      .load()

    val departmentData = sparkSession.sqlContext.read.format("jdbc")
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

    val joinTable = sparkSession.sqlContext.sql("select u.id, u.username, d.label " +
      "from users u join department d on u.id = d.id ")

    joinTable.filter(joinTable("username").endsWith("1")).show

    joinTable.show

    sparkSession.stop()

  } else {

    println("config.properties file not found.")

  }

}
