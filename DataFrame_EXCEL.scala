package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object DataFrame_EXCEL {
  def main(args: Array[String]): Unit = {

    val spark = org.apache.spark.sql.SparkSession.builder.master("local").appName("Spark EXCEL Reader").getOrCreate;

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    val empFile = "C:\\Users\\hp\\Downloads\\file_example_XLS_10.xls"

    val df = spark.read.format("com.crealytics.spark.excel")
      .option("sheetName", "Sheet1")
      .option("useHeader", "true")
      .option("treatEmptyValuesAsNulls", "false")
      .option("inferSchema", "true")
      .option("location", empFile)
      .option("addColorColumns", "False")
      .load(empFile)

    df.show(5 )
  }
}
