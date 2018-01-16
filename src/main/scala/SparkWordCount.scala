import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.io.File

object SparkWordCount extends App {

  if (File("config.properties").exists) {

    val input = new FileInputStream("config.properties")
    val properties = new Properties()

    properties.load(input)

    val InputFile = properties.getProperty("wordCountInputFile")
    val OutputDir = properties.getProperty("wordCountOutputDir")

    println(
      if (File(OutputDir).deleteRecursively()) {
        "Loading... cleaning directory... ready."
      } else {
        "Loading... ready."
      })

    val sparkConf = new SparkConf()
      .setAppName("Spark Word Count")
      .setMaster("local")
    val sc = new SparkContext(sparkConf)

    // this block is used for big files
    // val slices = if (args.length > 0) args(0).toInt else 2
    // val n = 100000 * slices
    // sc.textFile(InputFile, n)

    sc.textFile(InputFile)
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .saveAsTextFile(OutputDir)

    sc.stop()

  } else {

    println("config.properties file not found.")

  }

}
