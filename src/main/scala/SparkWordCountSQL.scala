import java.io.FileInputStream
import java.util.Properties

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.reflect.io.File

object SparkWordCountSQL extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Word(value: String)

  if (File("config.properties").exists) {

    val input = new FileInputStream("config.properties")
    val properties = new Properties()

    properties.load(input)

    val InputFile = properties.getProperty("wordCountInputFile")

    val spark = SparkSession.builder()
      .appName("Spark Word Count SQL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val wordDS = spark.sqlContext.read.textFile(InputFile)
      .flatMap(_.split("\\W+"))
      .map(Word)

    val result = wordDS.groupBy("value").count().orderBy(desc("count")).cache()

    result.show()

    spark.stop()

  } else {

    println("config.properties file not found.")

  }

}
