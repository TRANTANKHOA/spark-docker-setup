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
		val sampleRecord: String = mapSamples(table)
		source
			.where(col(table).isNotNull)
			.withColumn(table, from_json(
				col(table),
				schema_of_json(sampleRecord)
			)).select(col("__timestamp").as("update_date"), col(s"${table}.*"))
	}

	private val mapSamples: Map[String, String] = Map(
		"hubshare_forward" -> "{\"id\":1,\"hubshare_audit_id\":4978,\"share_id\":10363,\"forwarded_share_id\":0,\"created_at\":\"2018-04-09T03:44:02Z\"}",
		"btc_player_data" -> "{\"id\":46049,\"action\":\"view\",\"inserted_at\":\"2020-07-07T07:13:36Z\",\"recorded_time\":1594106015203000,\"file_id\":299442,\"payload\":\"\",\"slide_index\":0,\"ref_slide\":1,\"ref_slide_time\":5,\"user_id\":32213,\"company_id\":22,\"hubshare_id\":null,\"user_agent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36\",\"ip_address\":\"125.63.27.33\",\"session_id\":\"e38cf2b24c59b06a5ba9b8320b0ae235f2b5f28b\",\"client_device_id\":\"waedfb7bc8ed9411937972d55588a38f12b6165c4d8489f78ae9c0d3a896b530a3\",\"client_platform\":\"\",\"client_device_label\":\"Web App V4\",\"client_app_id\":\"\",\"client_version\":\"\",\"client_location\":{\"x\":151.0090863,\"y\":-33.8169516,\"wkb\":\"AQEAAADvKlpvSuBiQBs1ut6R6EDA\",\"srid\":null}}",
		"relationships_groups" -> "{\"group_id\":100290,\"object_id\":234,\"is_admin\":0,\"type\":\"user\",\"date_added\":\"2018-07-27T10:23:58Z\",\"last_updated\":\"2018-07-30T10:22:52Z\",\"deleted\":1,\"deleted_since\":\"2018-07-30T10:22:52Z\",\"company_id\":3}"
	)

	/**
	 * Spark Dataframes are immutable dataset without the concept of primary keys as with RDBMS technologies.
	 * Therefore, UPSERT can only be done by a left anti join with the existing table which require data shuffling.
	 * It is recommended that the tables are partitioned/bucketed using the join keys to minimise shuffling.
	 *
	 * @param currentRows
	 * @param newRows
	 * @param primaryKeys
	 * @return new dataset with newRows upserted into the currentRows
	 */
	def upsertUnion(currentRows: DataFrame, newRows: DataFrame, primaryKeys: Seq[String]): DataFrame = {
		val newDataWithSchemaCast: DataFrame = selectAndCastColumns(newRows, currentRows)
		currentRows
			.join(newDataWithSchemaCast, primaryKeys, joinType = "left_anti")
			.select(currentRows.columns.map(col): _*)
			.union(newDataWithSchemaCast)
	}

	/**
	 * Ensure that the `newRows` have the same schema as with `currentRows` up to each column's name and data type
	 *
	 * @param newRows
	 * @param currentRows
	 * @return same data as in newRows but with currentRows's schema
	 */
	def selectAndCastColumns(newRows: DataFrame, currentRows: DataFrame): DataFrame = {
		val newColumnsSet: Set[String] = newRows.columns.toSet
		val columns: Array[Column] = currentRows.columns.map(column => {
			if (!newColumnsSet.contains(column)) {
				lit(null).cast(currentRows.schema(column).dataType) as column
			} else {
				newRows(column).cast(currentRows.schema(column).dataType) as column
			}
		})
		newRows.select(columns: _*)
	}

}
