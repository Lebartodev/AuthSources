package com.lebartodev

import java.io.File

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

object Main {
  @transient lazy val logger: Logger = LogManager.getRootLogger
  val hitsAlgorithm: HitsAlgorithm = new HitsAlgorithm()
  // val pageRankAlgorithm: PageRankCustom = new PageRankCustom()

  def main(args: Array[String]): Unit = {
    val iter = 100
    val sparkSession = initSparkSession()
    logger.warn("Spark created")
    val graph = GraphUtils.load(session = sparkSession)
    logger.info("Graph loaded")

    val res = TrustRank.run(graph, iter )
    res.vertices.sortBy(_._2, ascending = false).take(10).foreach(println)

    sparkSession.stop()
  }

  def initSparkSession(): SparkSession = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val sparkBuilder = SparkSession.builder
      .appName("GraphX App")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()

    if (Config.LOCAL) {
      sparkBuilder.master("local[*]")
    }

    val spark = sparkBuilder.getOrCreate
    spark
  }

  //PR
  //(1488,60.18568990244379)
  //(4391,58.76839146329089)
  //(66,46.09349607735251)
  //(6427,44.665906421759026)
  //(4823,43.745034989883955)
  //(2078,41.96900400999156)
  //(0,40.52122445165459)
  //(1489,38.297215838620986)
  //(1617,35.207325361932)
  //(2408,35.1122914701594)


}
