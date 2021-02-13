package spark.streaming.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark._
import org.apache.spark.streaming._

object Application extends App {
  val logger: Logger = Logger.getLogger(this.getClass.getCanonicalName)
  def setStreamingLogLevels(): Unit = {
    val log4jInitialized: Boolean = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      logger.info("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
  setStreamingLogLevels()

  // Create a local StreamingContext with two working thread and batch interval of 1 second.
  // The master requires 2 cores to prevent a starvation scenario.

  val conf: SparkConf = new SparkConf().setMaster("spark://spark-master:7077").setAppName("NetworkWordCount")
  val ssc = new StreamingContext(conf, Seconds(1))

  val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
  val words: DStream[String] = lines.flatMap((_: String).split(" "))
  val pairs: DStream[(String, Int)] = words.map((word: String) => (word, 1))
  val wordCounts: DStream[(String, Int)] = pairs.reduceByKey((_: Int) + (_: Int))

  // Print the first ten elements of each RDD generated in this DStream to the console
  wordCounts.print()
  ssc.start()             // Start the computation
  ssc.awaitTermination()  // Wait for the computation to terminate
}
