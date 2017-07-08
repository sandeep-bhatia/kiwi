package kiwi
import java.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import org.apache.spark.graphx._

object ArticleConnections {

	def sortConnectedComponents(connectedComponents: org.apache.spark.graphx.Graph[VertexId, _]) : Seq[(VertexId, Long)] = {
			val componentCounts = connectedComponents.vertices.map(_._2).countByValue
			componentCounts.toSeq.sortBy(_._2).reverse
	}


	def createGraph(sparkContext: SparkContext, tenantName: String) : Boolean = {
		val articleSimiliarMappings = S3.getArticleIdSimiliars(sparkContext, tenantName)

		
		val vertices:RDD[(VertexId, String)] = articleSimiliarMappings.map(entry => {
			(entry._1.hashCode(), entry._1)
		})

		//This will give us all relevant edges of the graph
		val edges = articleSimiliarMappings.flatMap(entry => {
			//TODO : get the most relevant ones here 
			val similiars = entry._2.split(",")
			val edges = new ArrayBuffer[Edge[String]]()
			for(index <- 0 until similiars.length) {
				if (index < 3) {
					edges += new Edge[String](entry._1.hashCode(), similiars(index).hashCode(), String.format("%s-%s", entry._1, similiars(index)))
				}
			}

			edges
		}) 

		val defaultVertex = "0"
		val graph = org.apache.spark.graphx.Graph(vertices, edges, defaultVertex)
		val connectedComponents = graph.connectedComponents()

		if (Pretty.isTestLoad() == true) {
			val componentCounts = sortConnectedComponents(connectedComponents)
			Pretty.outputString(String.format("Component Counts By Size %s", componentCounts.size.toString))
			componentCounts.foreach(println)

			val verticesColl = connectedComponents.vertices.map(entry => {
				Pretty.outputString(String.format("Vertices are %s, %s", entry._1.toString, entry._2.toString))
			}).cache()
			Pretty.outputString(String.format("Printing count %s", verticesColl.count.toString))
			connectedComponents.edges.foreach(entry => {
				Pretty.outputString(String.format("with edge %s - %s, %s", entry.srcId.toString, entry.dstId.toString, entry.attr.toString ))
			})
		}

		val connectedArticleVertices = connectedComponents.edges.map(entry => {
			val splitValues = entry.attr.toString.split("-")
			(entry.srcId, splitValues)
		})

		val connectedArticleVerticesGrouped = connectedArticleVertices.reduceByKey((a, b) => {
			a ++ b
		})

		if (Pretty.isTestLoad() == true) {
			connectedArticleVerticesGrouped.foreach(entry => {
				Pretty.outputString(String.format("Grouped Vertices - with group Id %s, are %s", entry._1.toString, entry._2.distinct.mkString(",")))
			})
		}

		true
	}

	def findConnectedComponents() {
	}
}