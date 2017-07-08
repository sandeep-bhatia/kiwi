package kiwi

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.mllib.recommendation.{ALS, Rating}

object Merge extends PipelineStep {
	private var sparkContextObj: SparkContext = null
	private var sourceTableName: String = null
	private var userIdArticleMap:RDD[Tuple2[String, Array[String]]] = null
	private var mergedRecommendations:RDD[Tuple2[String, Array[String]]] = null

	def init(value: String) : PipelineStep = {
		sourceTableName = value
		this
	}

	def required(sparkContext: SparkContext, data: Any) {
		sparkContextObj = sparkContext

		data match {
  			case userIdArticles:RDD[Tuple2[String, Array[String]]] => {
  				Pretty.outputString("match success for recommendations")
  				userIdArticleMap = userIdArticles
  			}

  			case _ => {
  				Pretty.outputString("Cast for required in Recommendation Writer failed") 
  				throw new ClassCastException
  			}
	  	} 
	}

	def execute(tenantName: String) {
		var userStandard = 786
		if (Pretty.isTestLoad() == true) {
			userStandard = 1786
		}

		var disableCollaborativeMerge = false;

		if(sourceTableName == "" || sourceTableName == null || sourceTableName.length <= 0) {
			disableCollaborativeMerge = true;
		}

		var popularUserString = ""
		var popularArticles = Array[String]();

		popularUserString = Dynamo.readItemAttributeValue(sparkContextObj, String.format("%s.UserHistoryRecommendations", tenantName), "userId", userStandard.toString, "recommendations")
	    popularArticles = popularUserString.split(",")

    	if (Pretty.isTestLoad() == true) {
			Pretty.outputString(String.format("values in merge prior %s, %s", String.format("%s.UserHistoryRecommendations", tenantName), popularUserString));
		}

		mergedRecommendations = userIdArticleMap.map { case (userId, ratingsRaw) => {
			try {
				var finalRatingsCollector = ratingsRaw ++ popularArticles
				if(disableCollaborativeMerge == false) {
					val ratings = finalRatingsCollector
			    	val collaborativeStr = Dynamo.readItemAttributeValue(sparkContextObj, String.format(sourceTableName, tenantName), "userId", userId.toString.replace("\"", ""), "recommendations")
			    	
			    	if(collaborativeStr != null && collaborativeStr != "") {
			    		val collaborativeSeed = collaborativeStr.split(",")

			    		if(collaborativeSeed.length > 0) {
			    			val mergedArray = collaborativeSeed ++ ratings 
							finalRatingsCollector = mergedArray
						}
						else {
							finalRatingsCollector = ratings
						}
			    	} else {
			    		finalRatingsCollector = ratings
			    	}
		    	}

		    	if (finalRatingsCollector.length < 12) {
					val popularUserString = Dynamo.readItemAttributeValue(sparkContextObj, String.format("%s.UserHistoryRecommendations", tenantName), "userId", userStandard.toString, "recommendations")
					val popularArticles = popularUserString.split(",")
					finalRatingsCollector = finalRatingsCollector ++ popularArticles
					finalRatingsCollector = finalRatingsCollector.distinct
				}
				if (Pretty.isTestLoad() == true) {
					Pretty.outputString(String.format("values in merge ", userId.toString, finalRatingsCollector.mkString(",")))
				}

		    	(userId.toString, finalRatingsCollector)
			} catch {
				case e: Exception => {
				Pretty.outputString(String.format("error in record %s", userId.toString))
				Pretty.outputError(e.getMessage)
				("-1", Array[String]())
				}
			}}
		}.cache()

		if (Pretty.isTestLoad() == true) {
			val usersProducts = mergedRecommendations.map { entry =>
				Pretty.outputString(String.format("merged ratings %s, %s", entry._1.toString, entry._2.mkString("")))
				entry
			}.cache()
		}
	}

	def results() : Any = {
		mergedRecommendations
	}
}
