package kiwi
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import java.util._

object History extends PipelineStep {
private var sparkContextObj: SparkContext = null
private var rddUserArticle:RDD[Tuple2[String, String]] = null
private var historyRecommendations:RDD[Tuple2[String, Array[String]]] = null
private var historyTableSeed:String = ""
private var articleDateValues: RDD[Tuple2[String, Long]] = null

def init(value: String) : PipelineStep = {
	historyTableSeed = value
	this
}

def sortAndStoreTrending(tenantName: String, articleCounts:RDD[Tuple2[String, Long]]){
	Pretty.outputString(String.format("Count of articleDateValues articles %s", articleDateValues.count.toString))
	Pretty.outputString(String.format("Count of articleCounts articles %s", articleCounts.count.toString))
	val joinedArticleToDateCountMap = articleDateValues.join(articleCounts)

	val articleDateCountMap = joinedArticleToDateCountMap.reduceByKey((a, b) => {
		val date = if(a._1 > b._1) a._1 else b._1
		val count = if(a._1 > b._1) a._1 else b._1
		(date, count) 
	})
	val timeZone = TimeZone.getTimeZone("UTC")
	val calendar = Calendar.getInstance(timeZone)
	calendar.add(Calendar.DAY_OF_MONTH, -5)
	val threshold = calendar.getTimeInMillis();

	val trendingResult = articleDateCountMap.sortBy(_._2._2, false).filter(_._2._1 > threshold)
	Pretty.outputString(String.format("Count of trending articles %s", trendingResult.count.toString))
	var articles = new ArrayBuffer[String]()

	trendingResult.take(20).foreach(article => {
		articles += article._1
	})

	println("printing trending articles")
	if (Pretty.isTestLoad() == true) {

		articleDateCountMap.foreach(entry => {
			Pretty.outputString(String.format("article entry date count %s, %s, %s", entry._1, entry._2._1.toString, entry._2._2.toString))
		})
		articles.foreach(articleEntry => {
		 Pretty.outputString(String.format("Printing Article: %s", articleEntry))
		})
	}
	var articleList = articles.mkString(",")
	val historyTableRecommendations = "%s.UserHistoryRecommendations"
	
	if (Pretty.isTestLoad() == false) {
		Dynamo.putItemStringValue(sparkContextObj, String.format(historyTableRecommendations, tenantName), "userId", "786", "recommendations", articleList)
	} else {
		Pretty.outputString(String.format("%s: trending recos added", articleList))
		Pretty.outputString(String.format("Adding to the dynamo DB History recommendations table 1786 %s: %s", String.format(historyTableRecommendations, tenantName), articleList))
		Dynamo.putItemStringValue(sparkContextObj, String.format(historyTableRecommendations, tenantName), "userId", "1786", "recommendations", articleList)
	}
}

def setArticleDateValues(articleDates:RDD[Tuple2[String, Long]]) {
	articleDateValues = articleDates
}

def required(sparkContext: SparkContext, data: Any) = {
	sparkContextObj = sparkContext
	data match {
	  case userArticleList: RDD[Tuple2[String, String]] => {
	  rddUserArticle = userArticleList
	  }
	  case _ => {
	  Pretty.outputString("Cast for required in S3 Reader failed") 
	  throw new ClassCastException
	  }
	}
}

def removeQuotes(input: String): String = {
	var output = "";
	if (input.charAt(0) == '"') {
		output = input.substring(1, input.length)
	}
	if(output.charAt(output.length - 1) == '"') {
		output = output.substring(0, output.length - 1)
	}
	output
}

def execute(tenantName: String) {
	//calculate counts and their buckets
	val articleIdHits = rddUserArticle.map(record => {
	try {
		val userId = record._1.toString.replace("\"", "")
		val articleId = record._2.replace("\"", "")
		(articleId.toString, 1L)
	} catch {
		case e: Exception => {
			Pretty.outputString(String.format("error in record %s", record))
			("0", 1L)
		}
	}}
	)

	val bucketedEventCounts = articleIdHits.reduceByKey((a, b) => {
		if (Pretty.isTestLoad() == true) {
			Pretty.outputString(String.format("reduction stage: %s, %s", a.toString, b.toString));
		}

		a + b
	})
	sortAndStoreTrending(tenantName, bucketedEventCounts)

	val userClickStreams = rddUserArticle.reduceByKey((a, b) => {	
		a + "," + b
	})

	val dynamoDbHistoryTable = String.format("%s.UserHistory", tenantName);
	Dynamo.setupDynamoClientConnection()

	//Pretty.outputString(String.format("dynamoDbHistoryTable is %s", dynamoDbHistoryTable));
	//Pretty.outputString(String.format("UserClickStreamCount is %s", userClickStreams.count.toString()));
	historyRecommendations = userClickStreams.map(userHits => {
		val userId = userHits._1.toString.replace("\"", "")
		val hits = userHits._2.replace("\"", "").split(",")

		//Pretty.outputString(String.format("History entries are %s, %s", userId, hits));

		val historyStr = Dynamo.readItemAttributeValue(sparkContextObj, dynamoDbHistoryTable, "userId", userId, "history");
		val pastHistory = historyStr.split(",")
		var combinedHits = hits

		if(pastHistory.length > 1) {
			combinedHits = hits ++ pastHistory
		}

		var historySize = 20
		if(combinedHits.size > 20) {
			historySize = 20
		} else {
			historySize = combinedHits.size
		}

		val distinctHistory = combinedHits.distinct
		val newHistoryElements = distinctHistory.slice(0, distinctHistory.length - 1)
		if (Pretty.isTestLoad() == false) {
			Dynamo.putItemStringValue(sparkContextObj, dynamoDbHistoryTable, "userId", userId, "history", newHistoryElements.mkString(","))
		}
		(userId, newHistoryElements.toArray.distinct)
	});

	if (Pretty.isTestLoad() == true) {
		historyRecommendations.foreach(entry => {
			Pretty.outputString(String.format(">>>HISTORY > history user: %s, articles: %s", entry._1, entry._2.mkString(",")))
		})
		Pretty.outputString("history  count " + historyRecommendations.count.toString)
	}
}

def results() : Any = {
	historyRecommendations
}}


