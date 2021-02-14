package spark.streaming.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery
import spark.streaming.example.Spark.spark

object StreamJob {
	val logger: Logger = Logger.getLogger(this.getClass.getCanonicalName)
	spark.sparkContext.setLogLevel(Level.WARN.toString)
	lazy val input: DataFrame = spark.readStream
		.schema(BatchJob.input.schema)
		.option("maxFilesPerTrigger", 1)
		.json("/opt/spark-data/input/")

	lazy val table1: Table = new Table {
		override val name: String = "table1"
		override val partition: String = "__partition"
		override val output: DataFrame = Transformations.primaryTable(input)
	}

	lazy val hubshare_forward: Table = SecondaryTable(name = "hubshare_forward", partition = "share_id")
	lazy val btc_player_data: Table = SecondaryTable(name = "btc_player_data", partition = "company_id")
	lazy val relationships_groups: Table = SecondaryTable(name = "relationships_groups", partition = "company_id")

	case class SecondaryTable(name: String, partition: String) extends Table {
		lazy val output: DataFrame = Transformations.secondaryTable(table1.output, table = name)
	}

	trait Table {
		def name: String
		def partition: String
		def output: DataFrame

		def start(): StreamingQuery = output.writeStream
			.format("parquet")
			.outputMode("append")
			.option("path", s"/opt/spark-data/output/stream/$name/")
			.option("checkpointLocation", s"/opt/spark-data/output/stream/checkpoint/$name/")
			.partitionBy(partition)
			.start()
	}

	def run() {
		table1.start()
		hubshare_forward.start()
		btc_player_data.start()
		relationships_groups.start()
		spark.streams.awaitAnyTermination()
		logger.info("Finished Streaming")
	}
}
