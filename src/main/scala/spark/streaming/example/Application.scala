package spark.streaming.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming._

object Application extends App {
  val logger: Logger = Logger.getLogger(this.getClass.getCanonicalName)

  def setStreamingLogLevels(): Unit = {
    val log4jInitialized: Boolean = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark"s default logging, then we override the
      // logging level.
      logger.info("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }

  setStreamingLogLevels() // TODO this doesn"t seem to be working

  val conf: SparkConf = new SparkConf().setMaster("spark://spark-master:7077").setAppName("NetworkWordCount")
  val streamingContext = new StreamingContext(conf, Seconds(1))
  val sparkContext = new SparkContext(config = conf)
  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  val static = spark.read.json("/opt/spark-data/")
  val schema = StructType(List(
    StructField("op", StringType),
    StructField("after", StringType)
  ))

  static
    .withColumn("table", element_at(split(col("__topic"), "\\.", 3), 3))
    .withColumn("value", from_json(col("value"), schema))
    .select(
      col("__key"),
      col("__offset"),
      col("__topic"),
      col("table"),
      col("__partition"),
      col("__timestamp"),
      col("value.*")
    )
    .withColumnRenamed("op", "__action")
    .withColumn("date", to_date(col("__timestamp")))
    .transform(x => {x.printSchema();x})
    .show(5)

  streamingContext.start() // Start the computation
  streamingContext.awaitTermination() // Wait for the computation to terminate
}
