package kiwi

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable
import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.net.URLDecoder

object S3Reader extends PipelineStep {
	private var bucketName: String = ""
	private var sparkContextObj: SparkContext = null
	private var buckets: List[String] = null
	private var readValues: RDD[Tuple2[String, String]] = null

	def init(value: String) : PipelineStep = {
		bucketName = value	
		this
	}

	def required(sparkContext: SparkContext, data: Any) {
		sparkContextObj = sparkContext

		data match {
  			case bucketList: List[String] => {
  				buckets = bucketList
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
		var rdds:Array[RDD[Tuple2[String, String]]] = new Array[RDD[Tuple2[String, String]]](buckets.length)
		val rddsNonZero = mutable.ArrayBuffer[RDD[Tuple2[String, String]]]()

		var rddsArticleDates:Array[RDD[Tuple2[String, Long]]] = new Array[RDD[Tuple2[String, Long]]](buckets.length)
		val rddsNonZeroArticleDates = mutable.ArrayBuffer[RDD[Tuple2[String, Long]]]()
		Pretty.outputString(String.format("%s, bucket length for read", buckets.length.toString))
		for(index <- 0 to buckets.length - 1) {
			val readBucket = String.format("s3n://%s/%s", bucketName, buckets(index))
			var readRecords:RDD[String] = sparkContextObj.emptyRDD;
			readRecords = S3.readDataV2(sparkContextObj, readBucket);
			rdds(index) = readRecords.map(record => {
			try {
				val entries = record.split(",");
				(entries(2).replace("\"", ""), entries(1).replace("\"", ""))
				} catch {
			case e: Exception => {
				Pretty.outputError("error parsing record : " + record)
				("0", "0")
				}
			}
			});

			try {
				if(rdds(index) != null && rdds(index).count >  0) {
					rddsNonZero += rdds(index)
				}
			} catch {
		      case e: Exception => {
		        Pretty.outputError("zero count bucket encountered at: " + index.toString)
		      }
		    }

		    var dateVal = 0L;
    		rddsArticleDates(index) = readRecords.map(record => {
    			try {
					val entries = record.split(",");
					var addedonDate = entries(0);

					if(addedonDate == null || addedonDate.length == 0 || addedonDate == "{}") {
						dateVal = 0L	
					} else {
						//dateVal = addedonDate.replace("\"", "").toLong;
						//TODO : Added On Date is not available 
						dateVal = 0L;
					}
	    			val pair = (entries(1).replace("\"", ""), dateVal)
	    			Pretty.outputString("error parsing record : " + pair.toString)
	    			pair
	    		} catch {
				case e: Exception => {
					Pretty.outputError("error parsing record : " + record)
					Pretty.outputError(e.getMessage)
					("0", 0L)
					}
				}
			})


			try {
				if(rddsArticleDates(index) != null && rddsArticleDates(index).count >  0) {
					rddsNonZeroArticleDates += rddsArticleDates(index)
				}
			} catch {
		      case e: Exception => {
		        Pretty.outputError("zero count bucket encountered at: " + index.toString)
		      }
		    }
		}

		readValues = sparkContextObj.union(rddsNonZero)
		val articleDateValues = sparkContextObj.union(rddsNonZeroArticleDates)

		val fallbackDateMap = S3.getArticleIdDateMap(sparkContextObj, tenantName)
		val mergedArticleDateValues = articleDateValues.join(fallbackDateMap).map(entry => {

			if (entry._2._1 > entry._2._2) {
				if (Pretty.isTestLoad() == true) {
					Pretty.outputString(String.format("article %s: date %s ", entry._1, entry._2._1.toString))
				}

				(entry._1, entry._2._1)
			} else {
				if (Pretty.isTestLoad() == true) {
					Pretty.outputString(String.format("article %s: date %s ", entry._1, entry._2._2.toString))
				}
				
				(entry._1, entry._2._2)
			}
		})

		SortTransform.setArticleDateValues(mergedArticleDateValues)
		History.setArticleDateValues(mergedArticleDateValues)
		if (Pretty.isTestLoad() == true) {
			readValues.foreach(entry => {
				Pretty.outputString(String.format(">>>> user: %s, articles: %s", entry._1, entry._2))
			})
			Pretty.outputString("read  count " + readValues.count.toString)
		}
	}

	def results() : Any = {
		readValues
	}
}
