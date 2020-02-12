/* SimpleApp.scala */
package com.starfox.sparkaid

import org.apache.spark.sql.SparkSession

object TestApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()

    val inputFile = "src/test/resources/users.json" // Should be some file on your system
    val logData = spark.read.option("multiline", true).json(inputFile)

    NestedSchemaHandler().flattenAndExplode(logData).printSchema()

    spark.stop()
  }
}