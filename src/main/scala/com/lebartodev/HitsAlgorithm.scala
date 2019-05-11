package com.lebartodev

import com.lebartodev.Main._
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}

object HitsAlgorithm extends Serializable {

  case class VertexAttr(name: String, srcId: Long, authScore: Double, hubScore: Double)

  case class EdgeAttr(srcId: Long, dstId: Long)

  case class HitsMsg(authScore: Double, hubScore: Double)

  private def reducer(a: HitsMsg, b: HitsMsg): HitsMsg = HitsMsg(a.authScore + b.authScore, a.hubScore + b.hubScore)

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

    val delimeter = gx.edges.count()
    for (iter <- Range(1, maxIter)) {
      val msgs: VertexRDD[HitsMsg] = gx.aggregateMessages(
        ctx => {
          ctx.sendToDst(HitsMsg(0.0, ctx.srcAttr.hubScore))
          ctx.sendToSrc(HitsMsg(ctx.dstAttr.authScore, 0.0))
        }, reducer)
      // Update authority and hub scores of each node
      gx = gx.outerJoinVertices(msgs) {
        case (vID, vAttr, optMsg) => {
          val msg = optMsg.getOrElse(HitsMsg(1.0, 1.0))
          VertexAttr(vAttr.name, vAttr.srcId, if (msg.authScore == 0.0) 1.0 else msg.authScore, if (msg.hubScore == 0.0) 1.0 else msg.hubScore)
        }
      }

      gx = gx.mapVertices((vertexId: VertexId, ma: VertexAttr) => {
        VertexAttr(ma.name, ma.srcId, authScore = Math.sqrt(ma.authScore / (delimeter + iter)), hubScore = Math.sqrt(ma.hubScore / (delimeter + iter)))
      })


      logger.warn("mapping: " + iter)
    }
    gx
      .mapVertices((id, attr) => (attr.name, attr.authScore, attr.hubScore))
      .mapEdges(e => {
        "link"
      })

  }
}
