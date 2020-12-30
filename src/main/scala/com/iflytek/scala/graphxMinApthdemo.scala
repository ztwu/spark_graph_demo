package com.iflytek.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD

object graphxMinApthdemo {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("graphxMinApthdemo")
      .getOrCreate()
    val sc = sparkSession.sparkContext

//    val graph: Graph[Long, Double] =
//      GraphGenerators.logNormalGraph(sc, numVertices = 5).mapEdges(e => e.attr.toDouble)

    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )
    //边的数据类型 ED:Int
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )
    //构造 vertexRDD 和 edgeRDD
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
    //构造图 Graph[VD,ED]
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)
    graph.vertices.foreach(println)
    graph.edges.foreach(println)

    val sourceId: VertexId = 2L // The ultimate source

    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph: Graph[(Double, List[VertexId]), Double] =
      graph
        .mapEdges(e => e.attr.toDouble)
        .mapVertices((id, _) =>
          if (id == sourceId) (0.0, List[VertexId](sourceId))
          else (Double.PositiveInfinity, List[VertexId]())
        )

    initialGraph.vertices.foreach(println)

//    图的迭代计算
//    Pregel计算模型中有三个重要的函数，分别是vertexProgram、sendMessage和messageCombiner。
//    vertexProgram：用户定义的顶点运行程序。它作用于每一个顶点，负责接收进来的信息，并计算新的顶点值。
//    sendMsg：发送消息
//    mergeMsg：合并消息
    val sssp = initialGraph.pregel(
      (Double.PositiveInfinity, List[VertexId]()),
      Int.MaxValue,
      EdgeDirection.Out)(

        // Vertex Program
        (id, dist, newDist) => if (dist._1 < newDist._1) dist else newDist,

        // Send Message
        // 比较triplet.srcAttr + triplet.attr和triplet.dstAttr
        // 如果小于，则发送消息到目的顶点
        triplet => {
          if (triplet.srcAttr._1 < triplet.dstAttr._1 - triplet.attr) {
            Iterator((triplet.dstId, (triplet.srcAttr._1.toLong + triplet.attr, triplet.srcAttr._2 :+ triplet.dstId)))
          } else {
            Iterator.empty
          }
        },

        //Merge Message
        (a, b) => if (a._1 < b._1) a else b
    )

    println(sssp.vertices.collect.mkString("\n"))

    println("***********************************************")
    println("到某个v的最短路径")
    val end_ID: VertexId = 5L
    println(sssp.vertices.collect.filter{case(id,v) => id == end_ID}.mkString("\n"))

  }
}
