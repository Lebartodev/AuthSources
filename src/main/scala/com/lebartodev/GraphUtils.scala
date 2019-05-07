package com.lebartodev

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.neo4j.driver.v1._

object GraphUtils {
  private var neo4JDriver: Driver = null

  def load(session: SparkSession): Graph[String, String] = {
    if (Config.UPLOAD_TO_NEO4J) {
      neo4JDriver = GraphDatabase.driver("bolt://localhost:7687")
    }
    val fileName = getFileName(Config.FILE_NAME)
    val lines = session.sparkContext.textFile(fileName).cache
    val v: RDD[(VertexId, String)] = lines
      .filter(line => line.startsWith("n"))
      .map(line => line.substring(2))
      .map(line => line.split(' '))
      .map(chars => {
        val id = chars(0).toInt
        var name: String = ""
        for (i <- 1 until chars.length) {
          name = name + chars(i) + " "
        }
        (id, name)
      })
    //

    val e: RDD[Edge[String]] = lines
      .filter(line => line.startsWith("e"))
      .map(line => line.substring(2))
      .map(line => {
        val chars = line.split(' ')
        Edge(chars(0).toInt, chars(1).toInt, "link")
      })
    if (Config.UPLOAD_TO_NEO4J) {
      v.foreach(loadVertToNeo4J)
      e.foreach(loadEdgesToNeo4J)
    }

    Graph(v, e)
  }

  private def getFileName(s: String): String = {
    var res: String = null
    if (Config.LOCAL) {
      res = ""
    } else {
      res = "s3://mainlebartodevbucket/"
    }
    res = res + s
    res
  }


  private def loadVertToNeo4J(vert: (VertexId, String)): Unit = {
    val session = neo4JDriver.session
    session.writeTransaction(new VertexTransaction(vert))
  }

  private def loadEdgesToNeo4J(edge: Edge[String]): Unit = {
    val session = neo4JDriver.session
    session.writeTransaction(new EdgeTransaction(edge))
  }

  class VertexTransaction(vert: (VertexId, String)) extends TransactionWork[String] {

    override def execute(transaction: Transaction): String = {
      transaction.run("MERGE (p:Page {title: '" + vert._2 + "', id: '" + vert._1 + "'}) return 'kek'").single().get(0).asString()
    }
  }

  class EdgeTransaction(edge: Edge[String]) extends TransactionWork[String] {
    override def execute(transaction: Transaction): String = {
      transaction.run("MATCH (a:Page),(b:Page)\n" +
        "WHERE a.id = \"" + edge.srcId + "\" AND b.id=\"" + edge.dstId + "\"\n" +
        "MERGE (a)-[r:LINK]->(b)\n" +
        "RETURN 'kek'").single().get(0).asString()
    }
  }

}
