import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.{Graph, VertexId}
import scala.io.StdIn
import scala.util.control.Breaks._
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx._
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD

// Load the edges as a   graph
val   graph = GraphLoader.edgeListFile(sc, "/home/sahan/Desktop/Graph/facebook_combined.txt")

//Returns no of vertices in   graph
def numVertices(): Long = {
    graph.numVertices
  }

  // Returns the number of edges in the   graph
def numEdges(): Long = {
      graph.numEdges
  }

  // Returns the degree of a given vertex
def degree(vertexId: VertexId): Int = {
      graph.edges.filter(e => e.srcId == vertexId || e.dstId == vertexId).count().toInt
  }

//Top K vertices with highest degrees
def topKVertices(k: Int): Seq[(VertexId, Int)] = {
      graph.degrees.top(k)(Ordering.by((entry: (VertexId, Int)) => entry._2))
  }

// Returns the neighbors of a given vertex
def getNeighborVertices(vertexId: VertexId): List[(VertexId, Int)] = {
  val neighborIds =   graph.collectNeighborIds(EdgeDirection.Either).lookup(vertexId).head
  val neighborVertices =   graph.vertices.filter { case (id, _) => neighborIds.contains(id) }.collect()
  neighborVertices.toList
}

// Checks if an edge exists between two vertices
def edgeExists(graph: Graph[Int, Int], srcId: Long, dstId: Long): Boolean = {
  graph.edges.filter(e => (e.srcId == srcId && e.dstId == dstId) || (e.dstId == srcId && e.srcId == dstId)).count() > 0
}

// Returns the number of clusters in the graph
def numClusters(graph: Graph[Int, Int]): Long = {
  // Convert the graph to an undirected graph
  val undirectedGraph = graph.mapEdges(_ => 1).groupEdges(_ + _).mapVertices((_, _) => 1)

  // Run the connected components algorithm on the undirected graph
  val ccGraph = Graph(undirectedGraph.vertices, undirectedGraph.edges).connectedComponents()

  // Count the number of unique connected components
  ccGraph.vertices.map(_._2).distinct().count()
}

// Finds a cycle in the graph




def handle_query(input: Int): String = {
    input match {
        case 1 => 
            s"The number of vertices is ${  graph.numVertices}."
        case 2 =>
            s"The number of edges is ${  graph.numEdges}."
        case 3 =>
            print("Enter node id: ")
            val nodeId = scala.io.StdIn.readLine().toLong
            println(s"The node id you entered is ${nodeId}");
            s"The degree of node $nodeId is ${degree(nodeId)}."
        case 4 =>
            print("Enter the value of k: ")
            val k = scala.io.StdIn.readInt()
            printf("The value of k you enterd is %d",k)
            val topK = topKVertices(k)
            val topKString = topK.map { case (vertexId, degree) =>
                s"Vertex $vertexId has degree $degree"
            }.mkString("\n")
            s"The top $k vertices with highest degrees are:\n$topKString"
        
        case 5 => {
            println("Enter the ID of the vertex to get the neighbors of:")
            val vertexId = StdIn.readLong()
            val neighborVertices = getNeighborVertices(vertexId)
            s"The neighbors of vertex $vertexId are:\n${neighborVertices.mkString("\n")}"
            }
        case 6 => {
            print("\nEnter source vertex id: ")
            val srcId = scala.io.StdIn.readLong()
            println(s"\nThe source id you entered is ${srcId}")
            print("\nEnter destination vertex id: ")
            val dstId = scala.io.StdIn.readLong()
            println(s"\nThe destination id you entered is ${dstId}")
            val exists = edgeExists(graph, srcId, dstId)
            if (exists) {
                s"\nAn edge exists between vertex $srcId and vertex $dstId"
            } else {
                s"\nNo edge exists between vertex $srcId and vertex $dstId"
            }
        }
        case 7 =>{
            val numclusters = numClusters(graph)
            s"The number of clusters in the graph is ${numclusters}."}
        case 8 =>
            // Find triangles in the graph
            val triangleCount = graph.triangleCount().vertices
            val temp = triangleCount.map(_._2).reduce(_ + _) / 3
            s"Number of triangles : ${temp}"

        case 10 =>
            //rank
            val ranks = graph.pageRank(0.0001).vertices
            val output = ranks.collect().mkString("\n")
            s"${output}"


        case _ =>
            "Invalid input."
    }
}


 printf("\n1.Find the no of vertices in the graph.\n2.Find the no of edges in the graph\n3.Find the degree of a node.\n4.Find k top vertices(wrt degree)\n5.Find the neighbours of a given node\n6.Check whether an edge exists between two nodes\n7.Find no of clusters in the graph\n8.Find no  of triangles in the graph\n10.Find page rank of graph.\n")
 
 //while(true)
    // {
        printf("Enter a query number : ")
        val input: Int = scala.io.StdIn.readInt()
        // if(input==0) {
        //     printf("\nExiting...\n")
        //     break
        // }
        printf("\nThe query number you entered is %d\n",input)
        val result: String = handle_query(input)
    // }

