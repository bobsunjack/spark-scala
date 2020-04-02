package com.example.sparkscala.graph

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

object ex1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GraphXTest").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"),
      (3L, "Charles"), (4L, "Diane"), (5L, "Went to gym this morning")))
    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"),
      Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"),
      Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))
    val myGraph = Graph(myVertices, myEdges)
    myGraph.vertices.collect

    myGraph.mapTriplets(t => (t.attr, t.attr == "is-friends-with" &&
      t.srcAttr.toLowerCase.contains("a"))).triplets.collect

    myGraph.aggregateMessages[Int](_.sendToSrc(1), _ + _).collect

    myGraph.aggregateMessages[Int](_.sendToSrc(1),
      _ + _).join(myGraph.vertices).map(_._2.swap).collect

    myGraph.aggregateMessages[Int](_.sendToSrc(1),
      _ + _).rightOuterJoin(myGraph.vertices).map(_._2.swap).collect

    myGraph.aggregateMessages[Int](_.sendToSrc(1),
      _ + _).rightOuterJoin(myGraph.vertices).map(
      x => (x._2._2, x._2._1.getOrElse(0))).collect

    myGraph.vertices.saveAsObjectFile("myGraphVertices")
    myGraph.edges.saveAsObjectFile("myGraphEdges")
    val myGraph2 = Graph(
      sc.objectFile[Tuple2[VertexId, String]]("myGraphVertices"),
      sc.objectFile[Edge[String]]("myGraphEdges"))


    myGraph.vertices.coalesce(1, true).saveAsObjectFile("myGraphVertices")

    myGraph.vertices.map(x => {
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      mapper.registerModule(
        com.fasterxml.jackson.module.scala.DefaultScalaModule)
      val writer = new java.io.StringWriter()
      mapper.writeValue(writer, x)
      writer.toString
    }).coalesce(1, true).saveAsTextFile("myGraphVertices")


    val pw = new java.io.PrintWriter("gridGraph.gexf")
    // pw.write(toGexf(util.GraphGenerators.gridGraph(sc, 4, 4)))
    pw.close


    val vertices = sc.makeRDD(Seq(
      (1L, "Ann"), (2L, "Bill"), (3L, "Charles"), (4L, "Dianne")))
    val edges = sc.makeRDD(Seq(
      Edge(1L, 2L, "is-friends-with"), Edge(1L, 3L, "is-friends-with"),
      Edge(4L, 1L, "has-blocked"), Edge(2L, 3L, "has-blocked"),
      Edge(3L, 4L, "has-blocked")))
    val originalGraph = Graph(vertices, edges)
    val subgraph = originalGraph.subgraph(et => et.attr == "is-friends-with")
    // show vertices of subgraph – includes Dianne
    subgraph.vertices.foreach(println)
    // now call removeSingletons and show the resulting vertices
    removeSingletons(subgraph).vertices.foreach(println)



    val philosophers = Graph(
      sc.makeRDD(Seq(
        (1L, "Aristotle"),(2L,"Plato"),(3L,"Socrates"),(4L,"male"))),
      sc.makeRDD(Seq(
        Edge(2L,1L,"Influences"),
        Edge(3L,2L,"Influences"),
        Edge(3L,4L,"hasGender"))))
    val rdfGraph = Graph(
      sc.makeRDD(Seq(
        (1L,"wordnet_philosophers"),(2L,"Aristotle"),
        (3L,"Plato"),(4L,"Socrates"))),
      sc.makeRDD(Seq(
        Edge(2L,1L,"rdf:type"),
        Edge(3L,1L,"rdf:type"),
        Edge(4L,1L,"rdf:type"))))
    val combined = mergeGraphs(philosophers, rdfGraph)
    combined.triplets.foreach(
      t => println(s"${t.srcAttr} --- ${t.attr} ---> ${t.dstAttr}"))


  }

  import scala.reflect.ClassTag

  def removeSingletons[VD: ClassTag, ED: ClassTag](g: Graph[VD, ED]) =
    Graph(g.triplets.map(et => (et.srcId, et.srcAttr))
      .union(g.triplets.map(et => (et.dstId, et.dstAttr)))
      .distinct,
      g.edges)

  def mergeGraphs(g1: Graph[String, String], g2: Graph[String, String]) = {
    val v = g1.vertices.map(_._2).union(g2.vertices.map(_._2)).distinct
      .zipWithIndex

    def edgesWithNewVertexIds(g: Graph[String, String]) =
      g.triplets
        .map(et => (et.srcAttr, (et.attr, et.dstAttr)))
        .join(v)
        .map(x => (x._2._1._2, (x._2._2, x._2._1._1)))
        .join(v)
        .map(x => new Edge(x._2._1._1, x._2._2, x._2._1._2))

    Graph(v.map(_.swap),
      edgesWithNewVertexIds(g1).union(edgesWithNewVertexIds(g2)))
  }

}
