package com.lebartodev

import java.io.File

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession

object Main {
  @transient lazy val logger: Logger = LogManager.getRootLogger
  // val pageRankAlgorithm: PageRankCustom = new PageRankCustom()

  def main(args: Array[String]): Unit = {


    val iter = 100
    val sparkSession = initSparkSession()
    logger.warn("Spark created")
    val graph = GraphUtils.loadGoogle(session = sparkSession)
    logger.info("Graph loaded")

    timeHITS(graph, sparkSession)
    sparkSession.stop()

  }

  def saveInDegreeGraph(graph: Graph[String, String], sparkSession: SparkSession): Unit = {
    val count = graph.vertices.count()
    val result = graph.vertices.join(graph.inDegrees)
      .groupBy(a => a._2._2)
      .map(a => {
        val d = Math.log10(a._2.size)
        (Math.log10(a._1), d)
      })
      .sortBy(_._1, ascending = true) //.saveAsTextFile("result")

    val df = sparkSession.sqlContext.createDataFrame(result)
    df.write
      .format("com.crealytics.spark.excel")
      .option("useHeader", "false")
      .mode("append") // Optional, default: overwrite.
      .save("InDegreeLogGoogle.xlsx")
  }

  def savePageRankGraph(graph: Graph[String, String], sparkSession: SparkSession): Unit = {
    val count = graph.vertices.count()
    val res = PageRankUnCache.run(graph, 100, 0.15)
    val result = res.vertices
      .groupBy(a => a._2 - (a._2 % 0.001))
      .map(a => {
        val d = a._2.size.toDouble / count
        (a._1, d)
      })
    val df = sparkSession.sqlContext.createDataFrame(result)
    df.write
      .format("com.crealytics.spark.excel")
      .option("useHeader", "false")
      .mode("append") // Optional, default: overwrite.
      .save("PRGoogle.xlsx")
  }

  def timePageRankGraph(graph: Graph[String, String], sparkSession: SparkSession): Unit = {
    var start_ms = System.currentTimeMillis()
    val res = PageRankUnCache.run(graph, 10, 0.15)
    res.cache()
    println("Time : " + (System.currentTimeMillis() - start_ms))
  }

  def timeHITS(graph: Graph[String, String], sparkSession: SparkSession): Unit = {
    var start_ms = System.currentTimeMillis()
    val res = HitsAlgorithm.runHits(graph, 100)
    res.cache()
    println("Time : " + (System.currentTimeMillis() - start_ms))
  }

  def saveTrustRankGraph(graph: Graph[String, String], sparkSession: SparkSession): Unit = {
    val count = graph.vertices.count()
    val res = TrustRank.run(graph, 100)
    val result = res.vertices
      .groupBy(a => a._2 - (a._2 % 0.001))
      .map(a => {
        val d = a._2.size.toDouble / count
        (a._1, d)
      })
    val df = sparkSession.sqlContext.createDataFrame(result)
    df.write
      .format("com.crealytics.spark.excel")
      .option("useHeader", "false")
      .mode("append") // Optional, default: overwrite.
      .save("TrustGoogle.xlsx")
  }

  def saveHitsGraph(graph: Graph[String, String], sparkSession: SparkSession): Unit = {
    val count = graph.vertices.count()
    val res = HitsAlgorithm.runHits(graph, 10)
    val result = res.vertices
      .groupBy(a => a._2._2 - (a._2._2 % 0.0000001))
      .map(a => {
        val d = a._2.size.toDouble / count
        (a._1, d)
      })
    val df = sparkSession.sqlContext.createDataFrame(result)
    df.write
      .format("com.crealytics.spark.excel")
      .option("useHeader", "false")
      .mode("append") // Optional, default: overwrite.
      .save("HitsGoogle.xlsx")
  }

  def saveResult(graph: Graph[String, String], sparkSession: SparkSession): Unit = {
    //    val res = PageRankUnCache.run(graph, 30, 0.15)
    //    val result = res.vertices
    //      .sortBy(a => a._2, ascending = false).take(10)
    //    val df = sparkSession.sqlContext.createDataFrame(result)
    //    df.write
    //      .format("com.crealytics.spark.excel")
    //      .option("useHeader", "false")
    //      .mode("append") // Optional, default: overwrite.
    //      .save("ResultPrGoogle.xlsx")

    val resHits = HitsAlgorithm.runHits(graph, 20)
    resHits
      .vertices
      .sortBy(a => a._2._2, ascending = false)
      .foreach(println)
    //    val dfHits = sparkSession.sqlContext.createDataFrame(resultHits)
    //
    //    dfHits.write
    //      .format("com.crealytics.spark.excel")
    //      .option("useHeader", "false")
    //      .mode("append") // Optional, default: overwrite.
    //      .save("ResultHitsGoogle.xlsx")
  }

  def initSparkSession(): SparkSession = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val sparkBuilder = SparkSession.builder
      .appName("GraphX App")
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "4g")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()

    if (Config.LOCAL) {
      sparkBuilder.master("local[*]")
    }

    val spark = sparkBuilder.getOrCreate
    spark
  }
}
