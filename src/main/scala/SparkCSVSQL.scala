import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.sql.SparkSession

import scala.reflect.io.File

object SparkCSVSQL extends App {

  case class Baby(year: String, firstName: String, county: String, sex: String, count: String)

  if (File("config.properties").exists) {

    val input = new FileInputStream("config.properties")
    val properties = new Properties()

    properties.load(input)

    val spark = SparkSession.builder
      .appName("Spark CSV")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val babyNamesDS = spark.sqlContext.read.csv(properties.getProperty("babyNamesFileNoHeader"))
      .map(row => Baby(row(0).toString, row(1).toString, row(2).toString, row(3).toString, row(4).toString))
      .cache()

    babyNamesDS.select(babyNamesDS("year")).orderBy("year").distinct().show()

    spark.stop()

  } else {

    println("config.properties file not found.")

  }

}
