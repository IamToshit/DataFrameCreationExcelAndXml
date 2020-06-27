package org.example

import org.apache.spark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql


object DataFrame_XML {
  def main(args: Array[String]): Unit = {

    val spark = org.apache.spark.sql.SparkSession.builder.master("local").appName("Spark CSV Reader").getOrCreate;

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)


    val df = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "person")
      .load("C:\\Users\\hp\\Desktop\\persons.xml")//.show()

      df.createOrReplaceTempView("mytable")

    val dataFrame=spark.sql("Select dob_month from mytable where lastname = 'Smith'")
    //dataFrame.show(false)
    import spark.implicits._
    val df2 = dataFrame.withColumn("dob_month", $"dob_month".cast(sql.types.IntegerType))
    val b = df2.first().getInt(0)//.getBytes()
    //val d = b.map(x => x.get(0)).mkString(",")
    //val d = dataFrame()
    //val c = b[0]
    //println(c)
    println(b)
    //println(d)
    println(b.getClass)
  }
}
