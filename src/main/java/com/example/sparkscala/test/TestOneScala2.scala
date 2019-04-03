package com.example.sparkscala.test

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession

object TestOneScala2 {
  def test {
    val logFile = "c://README.md" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }


  def mongo():Unit= {

    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/capturedb2.capture_m")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/capturedb2.capture_m")
      .getOrCreate()

    val rdd = MongoSpark.load(spark)

    println(rdd.count)
    println(rdd.first.toString())

  }

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/capturedb2.capture_m")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/capturedb2.capture_m")
      .getOrCreate()

    val rdd = MongoSpark.load(spark)

    println(rdd.count)
    println(rdd.first.toString())

  }
}
