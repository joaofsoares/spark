import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.SparkContext

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

    val sc = new SparkContext("local[2]", "Spark Word Count")

    // this block is used for big files
    // val slices = if (args.length > 0) args(0).toInt else 2
    // val n = 100000 * slices
    // sc.textFile(InputFile, n)

    sc.textFile(InputFile)
      .flatMap(line => line.split("\\W+"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .saveAsTextFile(OutputDir)

    // other way to do the same thing
    //    sc.textFile(InputFile)
    //      .flatMap(line => line.split("\\W+"))
    //      .countByValue()
    //      .saveAsTextFile(OutputDir)

    sc.stop()

  } else {

    println("config.properties file not found.")

  }

}
