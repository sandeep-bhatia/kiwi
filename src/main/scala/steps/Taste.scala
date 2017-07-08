package kiwi

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashSet

object Taste extends PipelineStep {
private var bucketName: String = ""
private var sparkContextObj: SparkContext = null
private var buckets: List[String] = null
private var userIdArticleHistory: RDD[Tuple2[String, Array[String]]] = null
private var suggestedUserPreferenceArticles:RDD[Tuple2[String, Array[String]]] = null

def init(value: String) : PipelineStep = {
	Pretty.outputString(String.format("Value that is being set is %s for S3 Reader", value))
	bucketName = value
	this
}

def required(sparkContext: SparkContext, data: Any) {
	sparkContextObj = sparkContext

	data match {
	  case history:RDD[Tuple2[String, Array[String]]] => {
	  userIdArticleHistory = history
	}

	  case _ => {
	  throw new ClassCastException
	  }
 	} 
}

def execute(tenantName: String) {
	try {
		val readBucket = String.format("s3n://%s/similar_article_recommendations.json", String.format(bucketName, tenantName))
		val articleSimiliarMappings = S3.getArticleIdSimiliars(sparkContextObj, tenantName)

		val historyArticleUser = userIdArticleHistory.flatMap(entry => {
			val articleUsers = new ListBuffer[Tuple2[String, String]]()
			if (entry._2.length > 0) {
				entry._2.foreach(singleEntry => {
					val pair = (singleEntry, entry._1)
					articleUsers += pair
				})
			}
			articleUsers
		})

		/*****************************************************************************************************************/
		if (Pretty.isTestLoad() == true) {
			Pretty.outputString("article Users join count based on history " + historyArticleUser.count.toString)

			Pretty.outputString("article similiar mapping count based on the read values in S3" + articleSimiliarMappings.count.toString)

			articleSimiliarMappings.foreach(entry => {
				Pretty.outputString(String.format("article: %s, similiar: %s", entry._1, entry._2.toString))
			})

			historyArticleUser.foreach(entry => {
				Pretty.outputString(String.format("article: %s, user: %s", entry._1, entry._2))
			})
		}
		/*****************************************************************************************************************/

		val articleSimiliarUsersJoin = articleSimiliarMappings.join(historyArticleUser)
		if (Pretty.isTestLoad() == true) {
			Pretty.outputString("joined article table count " + articleSimiliarUsersJoin.count.toString)
		}

		val userToAccessedSimiliars:RDD[Tuple2[String, ArrayBuffer[Tuple2[String, String]]]] = articleSimiliarUsersJoin.flatMap(entry => {
			//user , accessed article, similiar ones)
			val historySimiliarsList = new ArrayBuffer[Tuple2[String, String]]()
			val pair = (entry._1, entry._2._1)
			val user = entry._2._2
			historySimiliarsList += pair
			val returnList = new ArrayBuffer[Tuple2[String, ArrayBuffer[Tuple2[String, String]]]]()
			val pairedUserToAccessedToSimiliarListTuple = (user, historySimiliarsList)
			returnList += pairedUserToAccessedToSimiliarListTuple
			returnList
		})

		if (Pretty.isTestLoad() == true) {
			Pretty.outputString("juserToAccessedSimiliars count " + userToAccessedSimiliars.count.toString)
		}

		val groupedUserToHistoryArticles = userToAccessedSimiliars.reduceByKey((a, b) => {
			a ++ b
		})

		if (Pretty.isTestLoad() == true) {
			Pretty.outputString("groupedUserToHistoryArticles table count " + groupedUserToHistoryArticles.count.toString)
		}

		suggestedUserPreferenceArticles = groupedUserToHistoryArticles.map(userIdArticleSimsTupleList => {
			val mutableBarredArticles = new HashSet[String]()
			val similiarBasedOnHistory = new ArrayBuffer[String]
			userIdArticleSimsTupleList._2.foreach(tupleArticleSimiliars => {
					mutableBarredArticles += tupleArticleSimiliars._1
			})
			
			userIdArticleSimsTupleList._2.foreach(tupleArticleSimiliars => {
			val currentSims = tupleArticleSimiliars._2.split(",")
			for(index <- 0 until currentSims.length)
				if(mutableBarredArticles.contains(currentSims(index)) == false) {
					similiarBasedOnHistory += currentSims(index)
				}
			})

			(userIdArticleSimsTupleList._1, similiarBasedOnHistory.toArray)
		}).cache()

		if (Pretty.isTestLoad() == true) {
			suggestedUserPreferenceArticles.foreach(entry => {
				Pretty.outputString(String.format("user: %s, articles: %s", entry._1, entry._2.mkString(",")))
			})
			Pretty.outputString("suggestedUserPreferenceArticles table count " + suggestedUserPreferenceArticles.count.toString)
		}
	} catch {
	     case e: Exception => {
	       Pretty.outputError(e.getMessage)
	     }
	}
}

def results() : Any = {
	suggestedUserPreferenceArticles
}}