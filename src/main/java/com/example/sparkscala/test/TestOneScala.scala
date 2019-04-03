package com.example.sparkscala.test

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import com.mongodb.spark._
import org.bson.Document

object TestOneScala {
  def test {
    val logFile = "c://README.md" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }


  def mongo(): Unit = {

    import org.apache.spark.sql.SparkSession

    import com.mongodb.spark.config._
    import org.apache.spark.sql._
    import scala.util.parsing.json.JSONObject
    import org.apache.spark.sql.types._

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Mingdao-Score")
      .set("spark.mongodb.input.uri", "mongodb://127.0.0.1/capturedb2.capture_m")
      .set("spark.mongodb.output.uri", "mongodb://127.0.0.1/capturedb2.capture_m")
    val sc = new SparkContext(conf)
    val rdd = MongoSpark.load(sc)

    println(rdd.count)
    println(rdd.first.toString())


    val aggregatedRdd = rdd.withPipeline(Seq(Document.parse("{ $match: { count : { $gt : 5 } } }")))
    println(aggregatedRdd.count)
    println(aggregatedRdd.first.toJson)

  }

  def test2: Unit = {
    import com.mongodb.spark._
    import org.apache.spark.{SparkConf, SparkContext}
    import org.bson._


    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Mingdao-Score")
      //同时还支持mongo驱动的readPreference配置, 可以只从secondary读取数据
      .set("spark.mongodb.input.uri", "mongodb://xxx.xxx.xxx.xxx:27017,xxx.xxx.xxx:27017,xxx.xxx.xxx:27017/inputDB.collectionName")
      .set("spark.mongodb.output.uri", "mongodb://xxx.xxx.xxx.xxx:27017,xxx.xxx.xxx:27017,xxx.xxx.xxx:27017/outputDB.collectionName")

    val sc = new SparkContext(conf)
    // 创建rdd
    val originRDD = MongoSpark.load(sc)

    // 构造查询
    val dateQuery = new BsonDocument()
      .append("$gte", new BsonDateTime(1))
      .append("$lt", new BsonDateTime(2))
    val matchQuery = new Document("$match", BsonDocument.parse("{\"type\":\"1\"}"))

    // 构造Projection
    val projection1 = new BsonDocument("$project", BsonDocument.parse("{\"userid\":\"$userid\",\"message\":\"$message\"}"))

    val aggregatedRDD = originRDD.withPipeline(Seq(matchQuery, projection1))

    //比如计算用户的消息字符数
    val rdd1 = aggregatedRDD.keyBy(x => {
      Map(
        "userid" -> x.get("userid")
      )
    })

    val rdd2 = rdd1.groupByKey.map(t => {
      (t._1, t._2.map(x => {
        x.getString("message").length
      }).sum)
    })

    rdd2.collect().foreach(x => {
      println(x)
    })

    //保持统计结果至MongoDB outputurl 所指定的数据库
    MongoSpark.save(rdd2)
  }


  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.SparkSession
    import com.mongodb.spark._
    import org.apache.spark.{SparkConf, SparkContext}
    import org.bson._


    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/capturedb2.capture_m")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/capturedb2.capture_m")
      .getOrCreate()
    import spark.implicits._
    import com.mongodb.spark._
    import spark.sqlContext.implicits._
    val rdd = MongoSpark.load(spark)
    import rdd.sqlContext.implicits._
    import scala.collection.JavaConverters._
    //val aggregatedRdd = rdd.withPipeline(Seq(Document.parse("{ $match: { test : { $gt : 5 } } }")))
    println(rdd.count)
    println(rdd.first.toString())

  }
}
