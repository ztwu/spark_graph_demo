package com.iflytek.scala

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PageRankTest01 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("app").master("local[*]").getOrCreate()

    val tweeters = Array((1L, ("Alice", 28)), (2L, ("Bob", 27)), (3L, ("Charlie", 65)), (4L, ("David", 42)), (5L, ("Ed", 55)), (6L, ("Fran", 50)))
    val vertexRDD: RDD[(Long, (String, Int))] = spark.sparkContext.parallelize(tweeters)

    val followRelations = Array(
      Edge[Int](2L, 1L, 7),
      Edge[Int](2L, 4L, 2),
      Edge[Int](3L, 2L, 4),
      Edge[Int](3L, 6L, 3),
      Edge[Int](4L, 1L, 1),
      Edge[Int](5L, 2L, 2),
      Edge[Int](5L, 3L, 8),
      Edge[Int](5L, 6L, 3))
    val edgeRDD = spark.sparkContext.parallelize(followRelations)

    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

    val ranks = graph.pageRank(0.0001)
    ranks.vertices.sortBy(_._2, false).collect.foreach(println)
    println("===============indegrees==============")
    // indegrees
    graph.inDegrees.foreach(println(_))
    println("===============outDegrees==============")
    graph.outDegrees.foreach(println(_))

  }
}
