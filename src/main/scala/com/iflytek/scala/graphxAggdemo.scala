package com.iflytek.scala

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object graphxAggdemo {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("LoadDataToNeo4j")
      .getOrCreate()
    val sc = sparkSession.sparkContext

    //设置顶点和边，注意顶点和边都是用元组定义的 Array
    //顶点的数据类型是 VD:(String,Int)
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

    case class User(name: String, age: Int, inDeg: Int, outDeg: Int)

    //创建一个新图，顶点 VD 的数据类型为 User，并从 graph 做类型转换
    val userGraph: Graph[User, Int] = graph.mapVertices {
      case (id, (name, age)) => User(name, age, 0, 0)
    }

    println("图的属性：")
    userGraph.vertices.collect.foreach(v => println(s"${v._2.name} inDeg: ${v._2.inDeg} outDeg: ${v._2.outDeg}"))

    println("聚合操作")
    println("**********************************************************")
    println("找出年纪最大的follower：")
    val oldestFollower: VertexRDD[(String, Int)] = userGraph.aggregateMessages[(String,
      Int)](
      // 将源顶点的属性发送给目标顶点，map 过程
      et => et.sendToDst((et.srcAttr.name,et.srcAttr.age)),
      // 得到最大follower，reduce 过程
      (a, b) => if (a._2 > b._2) a else b
    )

    println("聚合操作")
    println("**********************************************************")
    println("找出距离最远的顶点，Pregel基于对象")
    val g = Pregel(
      graph.mapVertices((vid, vd) => (0, vid)),
      (0, Long.MinValue),
      activeDirection = EdgeDirection.Out)(
        (id: VertexId, vd: (Int, Long), a: (Int, Long)) => math.max(vd._1, a._1) match {
          case vd._1 => vd
          case a._1 => a
        },
        (et: EdgeTriplet[(Int, Long), Int]) => {
          Iterator((et.dstId, (et.srcAttr._1 + 1+et.attr, et.srcAttr._2)))
        } ,
        (a: (Int, Long), b: (Int, Long)) => math.max(a._1, b._1) match {
          case a._1 => a
          case b._1 => b
        }
    )
    g.vertices.foreach(m=>println(s"原顶点${m._2._2}到目标顶点${m._1},最远经过${m._2._1}步"))

    // 面向对象
    val g2 = graph.mapVertices(
      (vid, vd) => (0, vid)).pregel((0, Long.MinValue),
      activeDirection = EdgeDirection.Out)(
        (id: VertexId, vd: (Int, Long), a: (Int, Long)) => math.max(vd._1, a._1) match {
          case vd._1 => vd
          case a._1 => a
        },
        (et: EdgeTriplet[(Int, Long), Int]) => Iterator((et.dstId, (et.srcAttr._1 + 1, et.srcAttr._2))),
        (a: (Int, Long), b: (Int, Long)) => math.max(a._1, b._1) match {
          case a._1 => a
          case b._1 => b
        }
    )
    g2.vertices.foreach(m=>println(s"原顶点${m._2._2}到目标顶点${m._1},最远经过${m._2._1}步"))

    sc.stop()

  }

}
