package spark.streaming.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import spark.streaming.example.Spark.spark

object StreamJob {
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

	val input: DataFrame = spark.readStream
		.schema(BatchJob.input.schema)
		.option("maxFilesPerTrigger", 1)
		.json("/opt/spark-data/")

	val table1: DataFrame = write {
		Transformations.primaryTable(input)
	}

	val hubshare_forward: DataFrame = write {
		Transformations.secondaryTable(table1, table = "hubshare_forward")
	}
	val btc_player_data: DataFrame = write {
		Transformations.secondaryTable(table1, table = "btc_player_data")
	}
	val relationships_groups: DataFrame = write {
		Transformations.secondaryTable(table1, table = "relationships_groups")
	}

	def write(dataFrame: DataFrame): DataFrame = {
		dataFrame.writeStream
			.outputMode("append")
			.format("console")
			.start().awaitTermination()
		dataFrame
	}

	def run(): Unit = {
		logger.info("Finished Streaming")
	}
}
