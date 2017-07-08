package kiwi
import java.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext

object SortTransform {
	private var articleDateValues: RDD[Tuple2[String, Long]] = null
	private var values:RDD[Tuple2[String, Array[String]]] = null
	private var articleTable:String = ""

	def setWriteTable(writeTable: String) {
		articleTable = writeTable
	}

	def setArticleDateValues(articleDates:RDD[Tuple2[String, Long]]) {
		articleDateValues = articleDates
	}

	def setArticleValues(recos:RDD[Tuple2[String, Array[String]]]) {
		values = recos
	}

	def articleSort(sparkContext: SparkContext, tenantName: String, tableName: String) : RDD[Tuple2[String, Array[String]]] = {
		var articleIdDateMap:RDD[Tuple2[String, Long]] = null
		Dynamo.setupDynamoClientConnection()

		if(articleDateValues != null) {
			articleIdDateMap = articleDateValues
		}

		val articleIdToUserMap = values.flatMap(values => {
			val articleIds = values._2
			val userIdArticleIdTuples = new Array[Tuple2[String, String]](articleIds.length)
			for (index <- 0 until articleIds.length) {
				userIdArticleIdTuples(index) = (articleIds(index), values._1)
			}

			userIdArticleIdTuples
		})

		val joinedArticleToDateUserMap = articleIdDateMap.join(articleIdToUserMap)
		/*****************************************************************************************************************/
		if (Pretty.isTestLoad() == true) {
			articleIdDateMap.foreach(entry => {
				Pretty.outputString(String.format("article: %s, date: %s", entry._1, entry._2.toString))
			})

			articleIdToUserMap.foreach(entry => {
				Pretty.outputString(String.format("article: %s, user: %s", entry._1, entry._2))
			})

			joinedArticleToDateUserMap.foreach(entry => {
				Pretty.outputString(String.format("article: %s, date: %s, user: %s", entry._1, entry._2._1.toString, entry._2._2))
			})
			
		}
		/*****************************************************************************************************************/
		val userToArticleWithDateMap = joinedArticleToDateUserMap.map(entry => {
			var listCreator = new ArrayBuffer[Tuple2[String, Long]]()
			val pair = (entry._1, entry._2._1)
			listCreator += pair
			(entry._2._2, listCreator)
		})

		val groupedUserArticleDateEntries = userToArticleWithDateMap.reduceByKey((a, b) => {
			a ++ b
		})

		val timeZone = TimeZone.getTimeZone("UTC")
		val calendar = Calendar.getInstance(timeZone)
		calendar.add(Calendar.DAY_OF_MONTH, -2)
		val threshold = calendar.getTimeInMillis();

		val sortedUserArticles = groupedUserArticleDateEntries.map(entry => {
			val freshArticlesNotMerged = entry._2.filter(listObject => {
				((listObject._2 > threshold) || (listObject._2 == 0))
			})

			val freshArticles = freshArticlesNotMerged

			if(freshArticles.length > 0) {
				val sorted = freshArticles.sortWith((a, b) => {
					a._2 > b._2
				})

				var sortedArticles = new Array[String](sorted.length)
				var index = 0
				var previous = "";
				sorted.foreach(entry => {
					if(entry._1 != previous) {
						sortedArticles(index) = entry._1
						index = index + 1
					}

					previous = entry._1
				})

				val processed = sortedArticles.filter(_ != null).distinct
				(entry._1, processed)
			} else {
				(entry._1, Array[String]())
			}
		})
		sortedUserArticles
	}
}