package com.iflytek.scala

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  *
  * 1）边分割(Edge-Cut)：每个顶点都存储一次，但有的边会被打断分到两台机器上。
  * 这样做的好处是节省存储空间；坏处是对图进行基于边的计算时，对于一条两个顶点被分到不同机器上的边来说，
  * 要跨机器通信传输数据，内网通信流量大
  *
  * 2）点分割(Vertex-Cut)：每条边只存储一次，都只会出现在一台机器上。邻居多的点会被复制到多台机器上，
  * 增加了存储开销，同时会引发数据同步问题。好处是可以大幅减少内网通信量
  *
  * 点分割方式存储图。这种存储方式特点是任何一条边只会出现在一台机器上，每个点有可能分布到不同的机器上。
  * 当点被分割到不同机器上时，是相同的镜像，但是有一个点作为主点,其他的点作为虚点，
  * 当点的数据发生变化时,先更新主点的数据，然后将所有更新好的数据发送到虚点所在的所有机器，更新虚点。
  * 这样做的好处是在边的存储上是没有冗余的，而且对于某个点与它的邻居的交互操作，只要满足交换律和结合律，
  * 就可以在不同的机器上面执行，网络开销较小。但是这种分割方式会存储多份点数据，更新点时，
  * 会发生网络传输，并且有可能出现同步问题。
  */
object graphxdemo {
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

    println("***********************************************")
    println("属性演示")
    println("**********************************************************")
    println("找出图中年龄大于 30 的顶点：")
    graph.vertices.filter { case (id, (name, age)) => age > 30 }.collect.foreach {
      case (id, (name, age)) => println(s"$name is $age")
    }
    graph.triplets.foreach(t => println(s"triplet:${t.srcId},${t.srcAttr},${t.dstId},${t.dstAttr},${t.attr}"))
    //边操作：找出图中属性大于 5 的边
    println("找出图中属性大于 5 的边：")
    graph.edges.filter(e => e.attr > 5).collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))

    println
    //triplets 操作，((srcId, srcAttr), (dstId, dstAttr), attr)
    println("列出边属性>5 的 tripltes：")
    for (triplet <- graph.triplets.filter(t => t.attr > 5).collect) {
      println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
    }

    println
    //Degrees 操作
    println("找出图中最大的出度、入度、度数：")
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }
    println("max of outDegrees:" + graph.outDegrees.reduce(max) + ", max of inDegrees:" + graph.inDegrees.reduce(max) + ", max of Degrees:" +
      graph.degrees.reduce(max))

    println
    println("**********************************************************")
    println("转换操作")
    println("**********************************************************")
    println("顶点的转换操作，顶点 age + 10：")
    graph.mapVertices { case (id, (name, age)) => (id, (name,
      age + 10))
    }.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))

    println
    println("边的转换操作，边的属性*2：")
    graph.mapEdges(e => e.attr * 2).edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))

    println
    println("**********************************************************")
    println("结构操作")
    println("**********************************************************")
    println("顶点年纪>30 的子图：")
    val subGraph = graph.subgraph(vpred = (id, vd) => vd._2 >= 30)
    println("子图所有顶点：")
    subGraph.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))

    println
    println("子图所有边：")
    subGraph.edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))

    sc.stop()

  }

}
