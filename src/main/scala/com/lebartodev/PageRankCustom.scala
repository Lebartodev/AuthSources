package com.lebartodev

import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

object PageRankCustom extends Logging {

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], numIter: Int,
                                      resetProb: Double = 0.15): Graph[Double, Double] = {

    // initialize pagerankGraph with each edge attribute
    // having weight 1 / outDegree and each vertex with attribute 1.0
    var rankGraph = graph
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
    val edgeTmp = rankGraph.mapTriplets(e => 1.0 / e.srcAttr, TripletFields.Src)
    val initialGraph = edgeTmp.mapVertices((id, attr) => resetProb)


    def vertexProgram(id: VertexId, attr: Double, msgSum: Double): Double = {
      attr + (1 - resetProb) * msgSum
    }

    def sendMessage(edge: EdgeTriplet[Double, Double]) = {
      Iterator((edge.dstId, edge.srcAttr * edge.attr))
    }

    def messageCombiner(a: Double, b: Double): Double = a + b

    val initialMessage = resetProb / (1.0 - resetProb)

    val result = Pregel(initialGraph, initialMessage, numIter)(
      vertexProgram, sendMessage, messageCombiner)
    result
  }

}
