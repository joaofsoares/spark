# spark-project

**Spark Project**

Spark version = 2.2.1

Scala version = 2.11.12

SBT version = 1.1.0

**Examples:**

 - Word Count
 
 - SparkPi
 
 - SparkMySQL

 - SparkJson

 - SparkCSV
 
**SBT Compile**

`sbt clean`

`sbt compile`

`sbt package` 
 
 
**Spark Cluster**

  **Master**

 - Start
 
 `spark/sbin $ ./start-master.sh `
 
 - Stop 
 
 `spark/sbin $ ./stop-master.sh` 
 
 - Access
 
 `http://localhost:8080`
 
 **Worker**
   
 - Start
 
 `spark/bin $ ./spark-class org.apache.spark.deploy.worker.Worker spark://localhost:7077`
 
 - Stop
 
 `Ctrl + C`
 
 **Deploy on Cluster**
 
 `spark/bin $ ./spark-submit --class "SparkPi" --master spark://localhost:7077 ./target/scala-2.11/spark-project_2.11-0.1.jar`
 
 `spark/bin $ ./spark-submit --class "SparkWordCount" --master spark://localhost:7077 ./target/scala-2.11/spark-project_2.11-0.1.jar`
 
 `spark/bin $ ./spark-submit --class "SparkMySQL" --master spark://localhost:7077 ./target/scala-2.11/spark-project_2.11-0.1.jar`

 `spark/bin $ ./spark-submit --class "SparkJson" --master spark://localhost:7077 ./target/scala-2.11/spark-project_2.11-0.1.jar`

 `spark/bin $ ./spark-submit --class "SparkCSV" --master spark://localhost:7077 ./target/scala-2.11/spark-project_2.11-0.1.jar`