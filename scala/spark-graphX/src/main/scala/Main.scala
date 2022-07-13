import java.io.Serializable

import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.graphx.{Edge, _}
import org.apache.spark.rdd.RDD

class VertexProperty()
case class Node(nodeId: String, nodeProperties: Option[Map[String, String]]) extends VertexProperty with Serializable



abstract class Graph[VD, ED] {
  val vertices: VertexRDD[VD]
  val edges: EdgeRDD[ED]
}
object Main extends App{

  val file_path = "src/main/resources/data.csv"
  val attributes = List("id", "description", "version",
    "next_version", "relation")
  val spark = SparkSession.builder.master("local[*]").getOrCreate()
  val sc: SparkContext = spark.sparkContext
  import spark.implicits._

  val srcDF = spark.read.option("header", true)
    .csv(file_path).select(attributes(0), attributes.drop(1):_*)

  import org.apache.spark.ml.feature.StringIndexer


  val indexer = new StringIndexer()
    .setInputCols(Array("version", "next_version"))
    .setOutputCols(Array("versionIndex", "next_versionIndex"))

  val indexed_srcDF = indexer.fit(srcDF).transform(srcDF)


  val graphDF = indexed_srcDF.map(row => {
    val r = scala.util.Random
    val srcId = row(5).asInstanceOf[Double].toLong
    val trgId = row(6).asInstanceOf[Double].toLong
    val srcNode = Map(srcId -> Node(row(0).asInstanceOf[String],
      Some(Map("description"-> row(1).asInstanceOf[String], "version" -> row(2).asInstanceOf[String]))))

    val trgNode = Map(trgId -> Node(row(0).asInstanceOf[String],
      Some(Map("description"-> row(1).asInstanceOf[String], "version" -> row(3).asInstanceOf[String]))))
    val edge = Map("source" -> srcId.toString, "target" -> trgId.toString, "relation" -> row(4).asInstanceOf[String])
    (srcNode, trgNode, edge)
  }).toDF("srcNodes", "trgNodes", "edges")



  // Define a default user in case there are relationship with missing user
  //val defaultUser = ("John Doe", "Missing", 0)
  // Build the initial Graph
  val nodes: RDD[(VertexId, Long)] = graphDF.select("srcNodes").union(graphDF.select("trgNodes"))
    .rdd.map(row => {
    val nodeMap = row(0).asInstanceOf[Map[Long, Node]]


    val key = (nodeMap.keys.toArray)
    (key(0), key(0))
  })


  val edges = graphDF.select("edges").rdd
    .map(x => {
      val edgeMap = x(0).asInstanceOf[Map[String, String]]
      Edge(edgeMap("source").toLong, edgeMap("target").toLong, edgeMap("relation"))
    })


  val graph = Graph(nodes, edges)


  val facts: RDD[String] = {
    //graph.triplets.map(triplet => triplet.srcAttr + " is the " + triplet.attr + " of " + triplet.dstAttr)
    graph.triplets.map(triplet => triplet.srcAttr + " is the " + triplet.attr + " of " + triplet.dstAttr)
  }

  facts.foreach(println)



}
