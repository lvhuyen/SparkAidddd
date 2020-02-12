/* SimpleApp.scala */
package com.starfox.sparkaid

import org.apache.spark.sql.SparkSession

object TestApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()

    val inputFile = "src/test/resources/doubleArray.json" // Should be some file on your system
    val logData = spark.read.option("multiline", true).json(inputFile)

    println(logData.count)
    val ret = NestedSchemaHandler().flattenAndExplode(logData)
    ret.printSchema()
    println(s"ABCDEF ${ret.count}")
    spark.stop()
  }
}