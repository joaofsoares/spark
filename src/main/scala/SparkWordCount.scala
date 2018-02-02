import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkWordCount extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val InputFile = "input_file_path"
  val OutputDir = "output_file_path" + System.currentTimeMillis()

  val sparkSession = SparkSession.builder
    .appName("Spark Word Count")
    .master("local[*]")
    .getOrCreate()

  val lines = sparkSession.sparkContext.textFile(InputFile)

  val eachWord = lines flatMap (_.split("\\W+"))

  val wordCountRDD = eachWord map ((_, 1)) reduceByKey (_ + _) sortBy(_._2, false) cache()

  wordCountRDD.foreach({
    case (word, count) => println(word + " : " + count)
  })

  wordCountRDD.saveAsTextFile(OutputDir)

  sparkSession.stop()

}
