package com.example.sparkscala.graph
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
object TestOne {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {

    // Assume the SparkContext has already been constructed
  //  val sc: SparkContext
  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("Mingdao-Score")
    val sc = new SparkContext(conf)
    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)
  //  val graph: Graph[(String, String), String] // Constructed from above
    // Count all users which are postdocs
    val count1=graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count
    print("------------------1-"+count1.toString())
    // Count all the edges where src > dst
    val count2= graph.edges.filter(e => e.srcId > e.dstId).count
    print("------------------2-"+count1.toString())
    print("end")

    // Use the triplets view to create an RDD of facts.
    val facts: RDD[String] =
      graph.triplets.map(triplet =>
        triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    facts.collect.foreach(println(_))
    println("end")
  }
}
