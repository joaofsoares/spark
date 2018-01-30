import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

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
