package com.example.sparkscala.graph
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
object test2 {
  def main(args: Array[String]): Unit = {

    // Create the context
    val sparkConf = new SparkConf().setAppName("myGraphPractice").setMaster("local[2]")
    val sc=new SparkContext(sparkConf)

    // 顶点RDD[顶点的id,顶点的属性值]
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // 边RDD[起始点id,终点id，边的属性（边的标注,边的权重等）]
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // 默认（缺失）用户
    //Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")

    //使用RDDs建立一个Graph（有许多建立Graph的数据来源和方法，后面会详细介绍）
    val graph = Graph(users, relationships, defaultUser)



    //读入数据文件
    val articles: RDD[String] = sc.textFile("E:/data/graphx/graphx-wiki-vertices.txt")
    val links: RDD[String] = sc.textFile("E:/data/graphx/graphx-wiki-edges.txt")

    //装载“顶点”和“边”RDD
    val vertices = articles.map { line =>
      val fields = line.split('\t')
      (fields(0).toLong, fields(1))
    }//注意第一列为vertexId，必须为Long，第二列为顶点属性，可以为任意类型，包括Map等序列。

    val edges = links.map { line =>
      val fields = line.split('\t')
      Edge(fields(0).toLong, fields(1).toLong, 1L)//起始点ID必须为Long，最后一个是属性，可以为任意类型
    }
    //建立图
    val graph2 = Graph(vertices, edges, "").persist()//自动使用apply方法建立图
  }
}
