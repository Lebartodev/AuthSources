package com.lebartodev

import org.neo4j.driver.v1.{Transaction, TransactionWork}

class NodeWork(val name: String, val id: Int) extends TransactionWork[String] {
  override def execute(transaction: Transaction): String = {
    val tran = "MERGE (p:Page {name: '" + name + "', id: '" + id + "'}) return 'kek'"
    val result = transaction.run(tran)
    result.single.get(0).asString
  }
}
