package spark.streaming.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark {
	val conf: SparkConf = new SparkConf().setMaster("spark://spark-master:7077").setAppName("App")
	val spark: SparkSession = SparkSession
		.builder()
		.config(conf)
		.getOrCreate()
}
