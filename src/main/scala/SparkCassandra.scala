import com.datastax.spark.connector.{SomeColumns, _}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkCassandra extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val conf = new SparkConf()
    .setAppName("Spark Streaming")
    .setMaster("local[2]")
    .set("spark.cassandra.connection.host", "127.0.0.1")

  val ssc = new StreamingContext(conf, Seconds(10))

  val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)

  val eachWord = lines.flatMap(_.split("\\W+"))

  val tupleWord = eachWord.map((_, 1))

  val wordCount = tupleWord.reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(300), Seconds(1))

  wordCount.foreachRDD((rdd, timestamp) => {
    rdd.cache()
    println("Writing " + rdd.count() + " rows to Cassandra...")
    rdd.saveToCassandra("default", "words", SomeColumns("word", "count"))
  })

  ssc.checkpoint("checkpoint")

  ssc.start()
  ssc.awaitTermination()
}
