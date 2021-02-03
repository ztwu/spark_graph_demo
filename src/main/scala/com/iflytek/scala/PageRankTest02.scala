package com.iflytek.scala

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object PageRankTest02 {
  def main(args: Array[String]): Unit = {
    //    val conf = new SparkConf().setAppName("app").setMaster("local[2]")
    //    val sc = SparkContext.getOrCreate(conf)

    val spark = SparkSession.builder().appName("app").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    // 1. 另一种构建Graph的方法：  GraphLoader.edgeListFile
    // 适用于不知道点和边的属性（默认为1），只有边 - 边的情况
    // 格式必须是 1空格2
    val graph = GraphLoader.edgeListFile(sc, "data/test02.txt").cache()
    //  graph.edges.collect().foreach(println(_))
    //  graph.vertices.collect().foreach(println(_))

    // 2. 根据边边关系，可做pagerank: (id,rankRate)
    val rankVertices = graph.pageRank(0.001).vertices
    // 3. verticeRDD
    val verticeRDD = sc.textFile("data/name.txt").map(x=>{
      val arr = x.split(",")
      // (id,NAME)
      (arr(0).toLong,arr(1))
    })

    // VD顶点的属性 = 1[INT]
    // (VertexId, VD, U) => VD
    // joinVertices方法只能改顶点的属性值，不能修改顶点属性类型
    //    graph.joinVertices(verticeRDD)((id,old,newval)=>(2)).vertices.foreach(println(_))

    // 4.使用RDD的join => (K,(V,W))
    // Rankgraph.vertices：(1,rate) VRDD：(1,Mike) => (Mike,rate)
    rankVertices.join(verticeRDD).map(f = {
      case (id, (rate, name)) => (name, rate)
    })
      .foreach(println)



  }

}
