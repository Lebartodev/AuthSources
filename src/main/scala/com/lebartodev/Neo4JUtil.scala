package com.lebartodev

import org.neo4j.driver.v1.{GraphDatabase, Transaction, TransactionWork}

import scala.io.Source

object Neo4JUtil {
  val edFile = "wiki-topcats.txt"
  val vFile = "wiki-topcats-page-names.txt"

  def main(args: Array[String]): Unit = {
    val filename = edFile
    initDatabase(edFile, vFile)
  }

  def initDatabase(edges: String, nodes: String): Unit = {
    val driver = GraphDatabase.driver("bolt://localhost:7687")
    val session = driver.session

    for (line <- Source.fromFile(nodes).getLines) {
      val chars = line.split(' ')
      val id = chars(0).toInt
      var name: String = ""
      for (i <- 1 until chars.length) {
        name = name + chars(i) + " "
      }
      name = name.replaceAll("'", "")
      session.writeTransaction(new NodeWork(name, id))
    }
    for (line <- Source.fromFile(edges).getLines) {
      val chars = line.split(' ')
      session.writeTransaction(new TransactionWork[String]() {
        override def execute(transaction: Transaction): String = {
          val tran = "MATCH (a:Page),(b:Page)" +
            "\nWHERE a.id = '" + chars(0).toInt + "' AND b.id = '" + chars(1).toInt + "'" +
            "\nCREATE (a)-[r:LINK]->(b)" +
            "\nRETURN type(r), r.name"
          val result = transaction.run(tran)
          result.single.get(0).asString
        }
      })
    }
  }
}
