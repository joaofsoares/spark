import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkCustomReceiver extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf: SparkConf = new SparkConf().setAppName("Spark Custom Receiver").setMaster("local[2]")

  val ssc = new StreamingContext(conf, Seconds(1))

  val lines = ssc.receiverStream(new CustomReceiver("localhost", 7777))

  val word = lines.flatMap(_.split("\\W+"))

  val tupleWord = word.map((_, 1))

  val wordCount = tupleWord.reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(300), Seconds(1))

  val sortedWordCount = wordCount.transform(rdd => rdd.sortBy(_._2, ascending = false))

  sortedWordCount.print()

  ssc.checkpoint("checkpoint")

  ssc.start()
  ssc.awaitTermination()

}
