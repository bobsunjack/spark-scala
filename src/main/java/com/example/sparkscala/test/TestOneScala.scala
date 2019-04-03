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



  def mongo2(): Unit = {

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
    val df = MongoSpark.load(spark)
    df.printSchema()
    df.filter(df("count") > 5).show()

    df.createOrReplaceTempView("child")

    val centenarians = spark.sql("SELECT * FROM child WHERE count >= 50")
    centenarians.show()
    MongoSpark.save(centenarians.write.option("collection", "hundredClub"))

  }

  def mongo3(): Unit = {

    import org.apache.spark.sql.SparkSession
    import com.mongodb.spark._
    import org.apache.spark.{SparkConf, SparkContext}
    import org.bson._


/*    val sc = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/capturedb2.stream_m")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/capturedb2.stream_m")
      .getOrCreate()*/

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("MongoSparkConnectorIntro")
      .set("spark.mongodb.input.uri", "mongodb://127.0.0.1/capturedb2.stream_m")
      .set("spark.mongodb.output.uri", "mongodb://127.0.0.1/capturedb2.stream_m")

    val sc = new SparkContext(conf)
    import com.mongodb.spark.sql._
    import org.apache.spark.streaming._

    val ssc = new StreamingContext(sc, Seconds(100))

    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.foreachRDD({ rdd =>
      val sparkSession = SparkSessionSingleton.getInstance(rdd.sparkContext)
      import sparkSession.implicits._

      val wordCounts = rdd.map({ case (word: String, count: Int) => WordCount(word, count) }).toDF()
      wordCounts.show()
      println("this is test-----------------------------------------------------------------")
      wordCounts.write.mode("append").mongo()
    })

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate

  }

  case class WordCount(word: String, count: Int)

  /** Lazily instantiated singleton instance of SQLContext */
  object SparkSessionSingleton {

    @transient private var instance: SparkSession = _

    def getInstance(sparkContext: SparkContext): SparkSession = {
      if (Option(instance).isEmpty) {
        instance = SparkSession.builder().getOrCreate()
      }
      instance
    }
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
