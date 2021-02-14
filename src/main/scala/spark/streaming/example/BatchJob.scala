package spark.streaming.example

import spark.streaming.example.Spark.spark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame

object BatchJob {
	val logger: Logger = Logger.getLogger(this.getClass.getCanonicalName)
	spark.sparkContext.setLogLevel(Level.WARN.toString)
	val input: DataFrame = spark.read.json("/opt/spark-data/input/")
	val table1: Table = new Table {
		override val name: String = "table1"
		override val partition: String = "__partition"
		override val output: DataFrame = Transformations.primaryTable(input).cache()
	}
	val hubshare_forward: SecondaryTable = SecondaryTable(name = "hubshare_forward", partition = "share_id")
	val btc_player_data: SecondaryTable = SecondaryTable(name = "btc_player_data", partition = "company_id")
	val relationships_groups: SecondaryTable = SecondaryTable(name = "relationships_groups", partition = "company_id")

	def run() {
		table1.write()
		hubshare_forward.write()
		btc_player_data.write()
		relationships_groups.write()
		logger.info("Finished Batching")
	}

	case class SecondaryTable(name: String, partition: String) extends Table {
		val output: DataFrame = Transformations.secondaryTable(table1.output, table = name)
	}

	abstract class Table {
		val name: String
		val partition: String
		val output: DataFrame

		def write() {
			output.write.partitionBy(partition)
				.mode("overwrite")
				.parquet(s"/opt/spark-data/output/batch/$name/")
		}
	}

}
