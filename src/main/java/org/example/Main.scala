package org.example


import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.sql.SparkSession

import java.util.Calendar

//noinspection DuplicatedCode
object Main {

  def main(args: Array[String]): Unit = {

    // Create a SparkSession
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Large Graph Analysis")
      .getOrCreate()
    spark.sparkContext.setLogLevel("FATAL")


    //val path = "input/com-youtube.ungraph.txt"
    val path = "input/Wiki-Vote.txt"
    //val path = "input/gplus_combined.txt"

    // Load the edges first
    val graph = GraphLoader.edgeListFile(spark.sparkContext, path)

    //input command in while loop
    while (true) {
      // GET COMMAND from stdin
      val command = scala.io.StdIn.readLine("Enter command: ")
      //switch case to handle commands
      command match {
        case "count" =>
          println(s"Number of vertices: ${graph.vertices.count()}")
          println(s"Number of edges: ${graph.edges.count()}")
        case "neighbors" =>
          val vertexId = scala.io.StdIn.readLine("Enter vertexId: ").toLong
          //check if vertex exists
          if (graph.vertices.filter(v => v._1 == vertexId).count() == 0) {
            println("Vertex does not exist")
          } else {
            //get neighbors
            //start time
            val startTime = Calendar.getInstance().getTimeInMillis
            val neighbors = graph.edges.filter(e => e.srcId == vertexId).map(_.dstId).collect()
            //end time
            val endTime = Calendar.getInstance().getTimeInMillis
            //print time taken
            println("Time taken: " + (endTime - startTime) + " ms")
            println(s"Neighbors of $vertexId: ${neighbors.mkString("(", ", ", ")")}")
          }
        case "triangles" =>
          //triangle count with graphx
          //start time
          val startTime = Calendar.getInstance().getTimeInMillis
          val numTriangles = TriangleCount.run(graph).vertices.map(_._2).reduce(_ + _) / 3
          //end time
          val endTime = Calendar.getInstance().getTimeInMillis
          //print time taken
          println("Time taken: " + (endTime - startTime) + " ms")
          println(s"Number of triangles in the graph: $numTriangles")
        case "pagerank" =>
          //input number of iterations
          val numIterations = scala.io.StdIn.readLine("Enter number of iterations: ").toInt
          //start time
          val startTime = Calendar.getInstance().getTimeInMillis
          val pageRankGraph = PageRank.run(graph, numIterations)
          val top10 = pageRankGraph.vertices.top(10)(Ordering.by(_._2))
          //end time
          val endTime = Calendar.getInstance().getTimeInMillis
          //print time taken
          println("Time taken: " + (endTime - startTime) + " ms")
          println("Top 10 vertices by PageRank:")
          top10.foreach { case (id, rank) => println(s"$id\t$rank") }
        //dynamic page rank
        case "dpr" =>
          //input tolerance
          val tolerance = scala.io.StdIn.readLine("Enter tolerance: ").toDouble
          //start time
          val startTime = Calendar.getInstance().getTimeInMillis
          val pageRankGraph = PageRank.runUntilConvergence(graph, tolerance)
          val top10 = pageRankGraph.vertices.top(10)(Ordering.by(_._2))
          //end time
          val endTime = Calendar.getInstance().getTimeInMillis
          //print time taken
          println("Time taken: " + (endTime - startTime) + " ms")
          println("Top 10 vertices by PageRank:")
          top10.foreach { case (id, rank) => println(s"$id\t$rank") }
        //single source shortest path
        case "sssp" =>
          //input source and destination vertex
          val source = scala.io.StdIn.readLine("Enter source vertex: ").toLong
          val destination = scala.io.StdIn.readLine("Enter destination vertex: ").toLong
          //check if source and destination are valid
          if (graph.vertices.filter(v => v._1 == source).count() == 0) {
            println("Source vertex does not exist")
          } else if (graph.vertices.filter(v => v._1 == destination).count() == 0) {
            println("Destination vertex does not exist")
          } else {
            //start time
            val startTime = Calendar.getInstance().getTimeInMillis
            //run sssp
            val sssp = ShortestPaths.run(graph, Seq(destination))
            //end time
            val endTime = Calendar.getInstance().getTimeInMillis
            //print time taken
            println("Time taken: " + (endTime - startTime) + " ms")
            //print shortest path
            val path = sssp.vertices.filter(v => v._1 == source).map(_._2).collect()(0)
            println(s"Shortest path from $source to $destination: ${path(destination)}")
          }
        //connected components
        case "cc" =>
          //start time
          val startTime = Calendar.getInstance().getTimeInMillis
          //run connected components
          val cc = ConnectedComponents.run(graph, 10).vertices
          //end time
          val endTime = Calendar.getInstance().getTimeInMillis
          //print time taken
          println("Time taken: " + (endTime - startTime) + " ms")
          //print number of connected components
          println("Number of connected components: " + cc.map(_._2).distinct().count())
          //print largest connected component
          val largestCC = cc.map(_._2).countByValue().maxBy(_._2)._1
          println("Largest connected component: " + largestCC)
          //print number of vertices in largest connected component
          println("Number of vertices in largest connected component: " + cc.filter(_._2 == largestCC).count())
        //strongly connected components
        case "scc" =>
          //start time
          val startTime = Calendar.getInstance().getTimeInMillis
          //run strongly connected components
          val scc = StronglyConnectedComponents.run(graph, 10).vertices
          //end time
          val endTime = Calendar.getInstance().getTimeInMillis
          //print time taken
          println("Time taken: " + (endTime - startTime) + " ms")
          //print number of strongly connected components
          println("Number of strongly connected components: " + scc.map(_._2).distinct().count())
          //print largest strongly connected component
          val largestSCC = scc.map(_._2).countByValue().maxBy(_._2)._1
          println("Largest strongly connected component: " + largestSCC)
          //print number of vertices in largest strongly connected component
          println("Number of vertices in largest strongly connected component: " + scc.filter(_._2 == largestSCC).count())
        //label propagation
        case "lp" =>
          //start time
          val startTime = Calendar.getInstance().getTimeInMillis
          //run label propagation
          val lp = LabelPropagation.run(graph, 5)
          //end time
          val endTime = Calendar.getInstance().getTimeInMillis
          //print time taken
          println("Time taken: " + (endTime - startTime) + " ms")
          //print number of connected components
          println("Number of connected components: " + lp.vertices.map(_._2).distinct().count())

        //exit
        case "exit" =>
          println("Exiting...")
          spark.stop()
          System.exit(0)
        case _ => println("Invalid command")
      }
    }

  }
}