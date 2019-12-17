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

    myGraph.mapTriplets(t => (t.attr, t.attr=="is-friends-with" &&
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
      sc.objectFile[Tuple2[VertexId,String]]("myGraphVertices"),
      sc.objectFile[Edge[String]]("myGraphEdges"))


    myGraph.vertices.coalesce(1,true).saveAsObjectFile("myGraphVertices")

    myGraph.vertices.map(x => {
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      mapper.registerModule(
        com.fasterxml.jackson.module.scala.DefaultScalaModule)
      val writer = new java.io.StringWriter()
      mapper.writeValue(writer, x)
      writer.toString
    }).coalesce(1,true).saveAsTextFile("myGraphVertices")


    val pw = new java.io.PrintWriter("gridGraph.gexf")
   // pw.write(toGexf(util.GraphGenerators.gridGraph(sc, 4, 4)))
    pw.close

  }

}
