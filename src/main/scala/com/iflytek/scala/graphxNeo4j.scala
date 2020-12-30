package com.iflytek.scala

import org.apache.spark.sql.SparkSession
import org.neo4j.spark.{Neo4j, Neo4jConfig}

object graphxNeo4j {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("LoadDataToNeo4j")
      .config("spark.neo4j.bolt.url", "bolt://192.168.56.101:7687")
      .config("spark.neo4j.bolt.user", "neo4j")
      .config("spark.neo4j.bolt.password", "123456")
      .getOrCreate();
    val sc = sparkSession.sparkContext
    Neo4j(sc)
      .cypher("match (c:user) return c.name, c.age, c.sex")
      .loadDataFrame.show()
    sparkSession.close()
  }
}
