package kiwi

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable
import org.json4s._
import org.json4s.jackson.JsonMethods._

object Recommendation extends PipelineStep {
	private var bucketName: String = ""
	private var sparkContextObj: SparkContext = null
	private var userIdArticleMap:RDD[Tuple2[String, Array[String]]] = null

	def init(value: String) : PipelineStep = {
		bucketName = value	
		this
	}

	def required(sparkContext: SparkContext, data: Any) {
		Pretty.outputString(String.format("Printing the sparkContext now"))
		sparkContextObj = sparkContext

		data match {
  			case recos:RDD[Tuple2[String, Array[String]]] => {
  				Pretty.outputString("match success for recommendations")
  				userIdArticleMap = recos
  			}

  			case _ => {
  				Pretty.outputString("Cast for required in Recommendation Writer failed") 
  				throw new ClassCastException
  			}
	  	} 
	}

	def execute(tenantName: String) {
		Dynamo.setupDynamoClientConnection()
		val writeTable = bucketName
		SortTransform.setWriteTable(writeTable)
		SortTransform.setArticleValues(userIdArticleMap)
		Pretty.outputString(String.format("user Id: Article Map - articles count %s",userIdArticleMap.count.toString))
		val articles = SortTransform.articleSort(sparkContextObj, tenantName, writeTable)
		Pretty.outputString(String.format("%s: the - articles count",articles.count.toString))
		if (Pretty.isTestLoad() == true) {
			Pretty.outputString(String.format("%s: table name and articles count %s", String.format(writeTable, tenantName), articles.count.toString))
		}
		articles.foreach(entry => {
				var finalArticleList = entry._2;
				if (finalArticleList.length < 20) {
					val popularUserString = Dynamo.readItemAttributeValue(sparkContextObj, String.format("%s.UserHistoryRecommendations", tenantName), "userId", "786", "recommendations")
					val popularArticles = popularUserString.split(",")
					finalArticleList = finalArticleList ++ popularArticles
					finalArticleList = finalArticleList.distinct
				}
				val articlesCat = finalArticleList.mkString(",")

				if (Pretty.isTestLoad() == true) { 
					Pretty.outputString(String.format("%s: recos added", articlesCat))
					Pretty.outputString(String.format("Adding to the dynamo %s DB table %s: %s", String.format(writeTable, tenantName), entry._1, articlesCat))
				}

				if (Pretty.isTestLoad() == false && Pretty.isDebugEnabled() == false) {
					Dynamo.putItemStringValue(sparkContextObj, String.format(writeTable, tenantName), "userId", entry._1, "recommendations", articlesCat)
				}
		})

		userIdArticleMap = articles;
	}

	def results() : Any = {
		userIdArticleMap
	}
}