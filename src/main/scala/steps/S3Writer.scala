package kiwi

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.mllib.recommendation.{ALS, Rating}

object S3Writer extends PipelineStep {
	private var bucketName: String = ""
	private var sparkContextObj: SparkContext = null
	private var recommendations:RDD[Tuple2[String, Array[String]]] = null
	private var countArticles:RDD[Tuple2[String, Int]] = null

	def init(value: String) : PipelineStep = {
		bucketName = value	
		this
	}

	def required(sparkContext: SparkContext, data: Any) {
		sparkContextObj = sparkContext

		Pretty.outputString("Pipeline context is :" + Pipeline.getPipelineContext())
		if(Pipeline.getPipelineContext() == "standard" || Pipeline.getPipelineContext() == "taste" || Pipeline.getPipelineContext() == "tasteV2") {
			data match {
	  			case recos:RDD[Tuple2[String, Array[String]]] => {
	  				recommendations = recos
	  			}

	  			case _ => {
	  				Pretty.outputString("Cast for required in S3 Writer failed") 
	  				throw new ClassCastException
	  			}
	  		} 
  		}

  		if(Pipeline.getPipelineContext() == "count") {
	  		data match {
	  			case counts:RDD[Tuple2[String, Int]] => {
	  				countArticles = counts
	  			}

	  			case _ => {
	  				Pretty.outputString("Cast for required in S3 Writer failed") 
	  				throw new ClassCastException
	  			}
	  			
			}
		}
	}

	def execute(tenantName: String) {
		if(countArticles != null) {
			S3Client.setupS3Client()
			val currentTimeSecs = System.currentTimeMillis().toString
			val currentTimeReverseSecs = currentTimeSecs.reverse
			var countLastKnownTable = String.format(String.format("%s.Recommendations.Metadata", tenantName))
			var propertyName = "countDevo"
			Dynamo.setupDynamoClientConnection()

			if (Pretty.isTestLoad() == false) {
				countArticles.saveAsObjectFile(String.format("s3n://%s/%s/%s.COUNTPRODRDD", bucketName, tenantName, currentTimeReverseSecs))
				propertyName = "countProd"
			} else {
				countArticles.saveAsObjectFile(String.format("s3n://%s/%s/%s.COUNTDEVORDD", bucketName, tenantName, currentTimeReverseSecs))
				propertyName = "countDevo"
			}

			Dynamo.putItemStringValue(sparkContextObj, countLastKnownTable, "Property", propertyName, "Value", currentTimeReverseSecs)
		}

		if(recommendations != null) {
			S3Client.setupS3Client()
			val currentTimeSecs = System.currentTimeMillis().toString
			val currentTimeReverseSecs = currentTimeSecs.reverse
			var recLastKnownTable = String.format("%s.Recommendations.Metadata", tenantName)
			var propertyName = "userDevo"
			Dynamo.setupDynamoClientConnection()

			val recommendationMaps = recommendations.map(entry => {
				(entry._1, entry._2.mkString(","))
			})
			if (Pretty.isTestLoad() == false) {
				recommendationMaps.saveAsTextFile(String.format("s3n://%s/%s/%s%s.PRODRDD", bucketName, tenantName, currentTimeSecs, currentTimeReverseSecs))
				propertyName = "userProd"
			} else {
				recommendationMaps.saveAsTextFile(String.format("s3n://%s/%s/%s$s.DEVORDD", bucketName, tenantName, currentTimeSecs, currentTimeReverseSecs))
				propertyName = "userDevo"
			}
		}
	}

	def results() : Any = {
		true
	}
}
