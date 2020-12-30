package com.iflytek.scala

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object graphxJoindemo {
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
    val initialUserGraph: Graph[User, Int] = graph.mapVertices {
      case (id, (name, age)) => User(name, age, 0, 0)
    }

    println("图的属性：")
    initialUserGraph.vertices.collect.foreach(v => {
      println(s"${v._2.name} inDeg: ${v._2.inDeg} outDeg: ${v._2.outDeg}")
    })

//    (1) Graph.vertices：图中的所有顶点；
//    (2) Graph.edges：图中所有的边；
//    (3) Graph.triplets：由三部分组成，源顶点，目的顶点，以及两个顶点之间的边；
//    (4) Graph.degrees：图中所有顶点的度；
//    (5) Graph.inDegrees：图中所有顶点的入度；
//    (6) Graph.outDegrees：图中所有顶点的出度；

    //initialUserGraph 与 inDegrees、outDegrees（RDD）进行连接，并修改 initialUserGraph中 inDeg 值、outDeg 值
    val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
      case (id, u, inDegOpt) => {
        User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
      }
    }.outerJoinVertices(initialUserGraph.outDegrees) {
      case (id, u, outDegOpt) => {
        User(u.name, u.age, u.inDeg, outDegOpt.getOrElse(0))
      }
    }
    println("连接图的属性：")
    userGraph.vertices.collect.foreach(v => {
      println(s"${v._2.name} inDeg: ${v._2.inDeg} outDeg: ${v._2.outDeg}")
    })

    println("**********************************************************")
    println("找出年纪最大的follower：")
    val oldestFollower: VertexRDD[(String, Int)] = userGraph.aggregateMessages[(String,
      Int)](
      // 将源顶点的属性发送给目标顶点，map 过程
      et => et.sendToDst((et.srcAttr.name,et.srcAttr.age)),
      // 得到最大follower，reduce 过程
      (a, b) => if (a._2 > b._2) a else b
    )
    userGraph.vertices.leftJoin(oldestFollower) {
      (id, user, optOldestFollower) => optOldestFollower match {
        case None => s"${user.name} does not have any followers."
        case Some((name, age)) => s"${name} is the oldest follower of ${user.name}."
      }
    }.collect.foreach { case (id, str) => println(str) }

    sc.stop()

  }

}
