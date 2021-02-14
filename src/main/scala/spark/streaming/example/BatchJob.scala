package spark.streaming.example

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import spark.streaming.example.Spark.spark

object BatchJob {
	val logger: Logger = Logger.getLogger(this.getClass.getCanonicalName)
	val input: DataFrame = spark.read.json("/opt/spark-data/")
	val table1: DataFrame = Transformations.primaryTable(input).cache()
	val hubshare_forward: DataFrame = Transformations.secondaryTable(table1, table = "hubshare_forward")
	val btc_player_data: DataFrame = Transformations.secondaryTable(table1, table = "btc_player_data")
	val relationships_groups: DataFrame = Transformations.secondaryTable(table1, table = "relationships_groups")
	def run(): Unit = {
		relationships_groups.show(10)
		relationships_groups.printSchema()
		logger.info("Finished Batching")
	}
}
