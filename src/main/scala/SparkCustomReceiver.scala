import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class CustomReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  override def onStart(): Unit = {

    Future {
      receive()
    }

  }

  override def onStop(): Unit = {

    println("Stopping service")

  }

  private def receive(): Unit = {

    val socket = new Socket(host, port)

    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, "UTF-8"))

    var userInput: String = null

    userInput = reader.readLine()
    while (!isStopped && userInput != null) {
      store(userInput)
      userInput = reader.readLine()
    }

    reader.close()
    socket.close()

    restart("trying to connect again")

  }
}

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
