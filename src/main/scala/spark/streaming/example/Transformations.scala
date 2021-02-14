package spark.streaming.example

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Transformations {
	def primaryTable(input: DataFrame): DataFrame = {
		input
			.withColumn("value", from_json(col("value"), StructType(List(
				StructField("op", StringType),
				StructField("after", StringType)
			))))
			.select(
				col("__key"),
				col("__offset"),
				col("__topic"),
				col("__partition"),
				col("__timestamp"),
				col("value.*")
			)
			.withColumn("table", element_at(split(col("__topic"), "\\.", 3), 3))
			.transform((dataframe: DataFrame) => {
				List("hubshare_forward", "btc_player_data", "relationships_groups").foldLeft(dataframe)(
					(df: DataFrame, name: String) => df.withColumn(name, when(col("table").equalTo(name), col("after")).otherwise(null))
				)
			})
			.withColumnRenamed("op", "__action")
			.withColumn("date", to_date(col("__timestamp"))) // found in the example output, but not explained in document
	}
	def secondaryTable(source: DataFrame, table: String): DataFrame = {
		val dataFrame: DataFrame = source
			.where(col(table).isNotNull)
			.select(col(table), col("__timestamp").as("update_date"))
			.cache()
		val schema: Column = schema_of_json(dataFrame.select(table).takeAsList(1).get(0).getString(0))
		dataFrame.withColumn(table, from_json(
			col(table),
			schema
		))
	}
}
