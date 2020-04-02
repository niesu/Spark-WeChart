package com.offcn.wechat

import org.apache.spark.sql.{DataFrame, SparkSession}


object ParseJson {
	def main(args: Array[String]): Unit = {
		val spark: SparkSession = SparkSession.builder().appName("ParseJson").master("local[*]").getOrCreate()

		val df: DataFrame = spark.read.json("file/test.json")
		df.printSchema()
		df.createOrReplaceTempView("a")
		df.show()

//		spark.sql("select topicType from a").show()

		spark.stop()
	}
}
