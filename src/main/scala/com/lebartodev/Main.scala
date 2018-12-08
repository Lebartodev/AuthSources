package com.lebartodev

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Main {
  val edFile = "s3://mainlebartodevbucket/wiki-topcats.txt"
  val vFile = "s3://mainlebartodevbucket/wiki-topcats-page-names.txt"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("SparkJob").getOrCreate

    val graph = load(session = spark)
    val res = runHits(graph)
    res.vertices.saveAsTextFile("s3://mainlebartodevbucket/saved")

  }

  def load(session: SparkSession): Graph[String, String] = {

    val fileName = "s3://mainlebartodevbucket/wiki-topcats-page-names.txt"
    val lines = session.sparkContext.textFile(fileName).cache
    val v: RDD[(VertexId, String)] = lines.map(line => {
      val chars = line.split(' ')
      val id = chars(0).toInt
      var name: String = ""
      for (i <- 1 until chars.length) {
        name = name + chars(i) + " "
      }
      (id, name)
    })

    val fileNameEdges = "s3://mainlebartodevbucket/wiki-topcats.txt"
    val linesEdges = session.sparkContext.textFile(fileNameEdges).cache
    val e: RDD[Edge[String]] = linesEdges.map(line => {
      val chars = line.split(' ')
      Edge(chars(0).toInt, chars(1).toInt, "link")
    })
    Graph(v, e)
  }

  case class VertexAttr(name: String, srcId: Long, authScore: Double, hubScore: Double)

  case class EdgeAttr(srcId: Long, dstId: Long)

  case class HitsMsg(authScore: Double, hubScore: Double)

  def reducer(a: HitsMsg, b: HitsMsg): HitsMsg = HitsMsg(a.authScore + b.authScore, a.hubScore + b.hubScore)

  def runHits(g: Graph[String, String], maxIter: Int = 3): Graph[(String, Double, Double), String] = {
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
      val totalHubScores = gx.vertices
      val msgs: VertexRDD[HitsMsg] = gx.aggregateMessages(
        ctx => {
          ctx.sendToDst(HitsMsg(ctx.srcAttr.authScore, 0.0))
          ctx.sendToSrc(HitsMsg(0.0, ctx.dstAttr.hubScore))
        }, reducer)
      // Update authority and hub scores of each node
      gx = gx.outerJoinVertices(msgs) {
        case (vID, vAttr, optMsg) => {
          val msg = optMsg.getOrElse(HitsMsg(1.0, 1.0))
          VertexAttr(vAttr.name, vAttr.srcId, if (msg.authScore == 0.0) 1.0 else msg.authScore, if (msg.hubScore == 0.0) 1.0 else msg.hubScore)
        }
      }
      println("Iter ", iter)
    }
    gx.mapVertices((id, attr) => (attr.name, attr.authScore, attr.hubScore)).mapEdges(e => {
      "link"
    })

  }

}
