package com.lebartodev

import java.io.File

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.PageRank
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Main {
  val edFile = "wiki-topcats.txt"
  val vFile = "wiki-topcats-page-names.txt"
  val LOCAL = true
  @transient lazy val logger: Logger = LogManager.getRootLogger

  def main(args: Array[String]): Unit = {

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val sparkBuilder = SparkSession.builder
      .appName("GraphX App")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()

    if (LOCAL) {
      sparkBuilder.master("local[*]")
    }

    val spark = sparkBuilder.getOrCreate
    logger.warn("Spark created")
    val graph = testLoad(session = spark)

    logger.info("graph loaded")
    val res = runHits(graph)
    logger.info("hits completed")


    res.vertices.coalesce(1, true).sortBy(_._2._2, ascending = false).saveAsTextFile(getFileName("saved/" + System.currentTimeMillis()))

    val ranks = PageRank.run(graph, 3).vertices
    //
    val ranksByUsername = graph.outerJoinVertices(ranks) {
      case (vID, vAttr, optMsg) => {
        val msg = optMsg.getOrElse(0.0)
        (vAttr, msg)
      }
    }
    ranksByUsername.vertices.coalesce(1, true).sortBy(_._2._2, ascending = false).saveAsTextFile(getFileName("pagerank/" + System.currentTimeMillis()))
    logger.info("saved")


  }

  def load(session: SparkSession): Graph[String, String] = {

    val fileName = getFileName("wiki-topcats-page-names.txt")
    val lines = session.sparkContext.textFile(fileName).cache
    val v: RDD[(VertexId, String)] = lines.map(line => {
      val chars = line.split(' ')
      val id = chars(0).toInt
      var name: String = ""
      for (i <- 1 until chars.length) {
        name = name + chars(i) + " "
      }
      name = name.replaceAll("'", "")
      (id, name)
    })

    val fileNameEdges = getFileName("wiki-topcats.txt")
    val linesEdges = session.sparkContext.textFile(fileNameEdges).cache
    val e: RDD[Edge[String]] = linesEdges.map(line => {
      val chars = line.split(' ')
      Edge(chars(0).toInt, chars(1).toInt, "link")
    })
    Graph(v, e)
  }

  def testLoad(session: SparkSession): Graph[String, String] = {
    val users: RDD[(VertexId, String)] =
      session.sparkContext.parallelize(Array((3L, "rxin"), (7L, "jgonzal"),
        (5L, "prof"), (2L, "istoica")))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      session.sparkContext.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Build the initial Graph
    Graph(users, relationships)
  }

  case class VertexAttr(name: String, srcId: Long, authScore: Double, hubScore: Double)

  case class EdgeAttr(srcId: Long, dstId: Long)

  case class HitsMsg(authScore: Double, hubScore: Double)

  def reducer(a: HitsMsg, b: HitsMsg): HitsMsg = HitsMsg(a.authScore + b.authScore, a.hubScore + b.hubScore)

  def runHits(g: Graph[String, String], maxIter: Int = 10): Graph[(String, Double, Double), String] = {
    val gx1: Graph[VertexAttr, String] = g.mapVertices { case (id, attr) =>
      VertexAttr(attr, id, authScore = 1.0, hubScore = 1.0)
    }

    // Convert edge attributes to nice case classes.
    var gx: Graph[VertexAttr, EdgeAttr] = gx1.mapEdges {
      e =>
        val src = e.srcId
        val dst = e.dstId
        EdgeAttr(src, dst)
    }


    for (iter <- Range(1, maxIter)) {
      val msgs: VertexRDD[HitsMsg] = gx.aggregateMessages(
        ctx => {
          ctx.sendToDst(HitsMsg(0.0,ctx.srcAttr.hubScore))
          ctx.sendToSrc(HitsMsg(ctx.dstAttr.authScore,0.0))
        }, reducer)
      // Update authority and hub scores of each node
      gx = gx.outerJoinVertices(msgs) {
        case (vID, vAttr, optMsg) => {
          val msg = optMsg.getOrElse(HitsMsg(1.0, 1.0))
          VertexAttr(vAttr.name, vAttr.srcId, if (msg.authScore == 0.0) 1.0 else msg.authScore, if (msg.hubScore == 0.0) 1.0 else msg.hubScore)
        }
      }
      val authsum = gx.vertices.map(_._2.authScore).sum()
      val hubsum = gx.vertices.map(_._2.hubScore).sum()

      gx = gx.mapVertices((vertexId: VertexId, ma: Main.VertexAttr) => {
        VertexAttr(ma.name, ma.srcId, authScore = Math.sqrt(ma.authScore / authsum), hubScore = Math.sqrt(ma.hubScore / hubsum))
      })


      logger.warn("mapping: " + iter)
    }
    gx.mapVertices((id, attr) => (attr.name, attr.authScore, attr.hubScore)).mapEdges(e => {
      "link"
    })

  }

  private def getFileName(s: String): String = {
    var res: String = null
    if (LOCAL) {
      res = ""
    } else {
      res = "s3://mainlebartodevbucket/"
    }
    res = res + s
    res
  }

}
