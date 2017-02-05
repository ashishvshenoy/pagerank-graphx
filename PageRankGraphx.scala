/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println

// $example on$
import org.apache.spark.graphx.GraphLoader
// $example off$
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import scala.reflect.ClassTag

/**
 * A PageRank example on social network dataset
 * Run with
 * {{{
 * bin/run-example graphx.PageRankExample
 * }}}
 */
object PageRankExample {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName("PageRankGraphx").config("spark.driver.memory","8g")
      .config("spark.eventLog.enabled","true")
      .config("spark.eventLog.dir","hdfs://10.254.0.33:8020/user/ubuntu/applicationHistory")
      .config("spark.executor.memory","8g")
      .config("spark.executor.cores","4")
      .config("spark.task.cpus","1")
      .config("spark.executor.instances","4")
      .config("spark.default.parallelism","16")
      .master("spark://10.254.0.33:7077")
      .getOrCreate()
    val sc = spark.sparkContext

    // $example on$
    // Load the edges as a graph
    val graph = GraphLoader.edgeListFile(sc, "soc-LiveJournal1.txt")    // Run PageRank
    //val ranks = graph.pageRank(0.0001).vertices

    val resetProb = 0.15
    val numIter = 20

    val pagerankGraph: Graph[Double, Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees){
      (vid, vdata, deg) => deg.getOrElse(0)
    }
    // Set the weight on the edges based on the degree
    .mapTriplets( e => 1.0 / e.srcAttr )
    // Set the vertex attributes to the initial pagerank values
    .mapVertices( (id, attr) => 1.0 )
    .cache()

    def vertexProgram(id: VertexId, attr: Double, msgSum: Double): Double = resetProb + (1.0 - resetProb) * msgSum
    def sendMessage(edge: EdgeTriplet[Double, Double]) = Iterator((edge.dstId, edge.srcAttr * edge.attr))
    def messageCombiner(a: Double, b: Double): Double = a + b
    // The initial message received by all vertices in PageRank
    val initialMessage = 0.0

    val ranks = Pregel(pagerankGraph, initialMessage, numIter)(vertexProgram, sendMessage, messageCombiner).vertices

    // Print the result
    ranks.saveAsTextFile("pagerankgraphx_output")
    // $example off$
    spark.stop()
  }
}
// scalastyle:on println
