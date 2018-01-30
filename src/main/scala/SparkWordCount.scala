import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val InputFile = "input_file_path"
  val OutputDir = "output_file_path" + System.currentTimeMillis()

  val sparkConf = new SparkConf().setAppName("Spark Word Count").setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  // this block is used for big files
  // val slices = if (args.length > 0) args(0).toInt else 2
  // val n = 100000 * slices
  // sc.textFile(InputFile, n)

  val eachWord = sc.textFile(InputFile).flatMap(line => line.split("\\W+"))

  val wordCount = eachWord.map((_, 1))

  val totalWordCount = wordCount.reduceByKey(_ + _)

  totalWordCount.saveAsTextFile(OutputDir)

  // other way to do the same thing
  //    sc.textFile(InputFile)
  //      .flatMap(line => line.split("\\W+"))
  //      .countByValue()
  //      .saveAsTextFile(OutputDir)

  sc.stop()

}
