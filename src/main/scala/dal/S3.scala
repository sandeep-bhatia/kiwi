	package kiwi

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import java.text.SimpleDateFormat
object S3 {
 	
 	def setupHadoopConfiguration (sparkContext : SparkContext) {
    	
    	val hadoopConf = sparkContext.hadoopConfiguration
    	hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    	hadoopConf.set("fs.s3n.awsAccessKeyId", "AKIAJIQS5GRC32IWX3AA")
    	hadoopConf.set("fs.s3n.awsSecretAccessKey", "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")
  		
  	}

  	def readCountsData(sparkContext: SparkContext, bucketString: String) : RDD[Tuple2[String, String]] = {
 		setupHadoopConfiguration(sparkContext)
 		var articleCounts = sparkContext.wholeTextFiles(bucketString)
 		var countsMap = articleCounts.map(articleCount => {
 				(articleCount._1, articleCount._2)
 		})

 		countsMap
 	}

 	def readUserRecommendationsData(sparkContext: SparkContext, bucketString: String) : RDD[Tuple2[String, Array[String]]] = {
 		setupHadoopConfiguration(sparkContext)
 		val recommendationData = sparkContext.wholeTextFiles(bucketString)
 		val recommendations = recommendationData.map(recommendationsUser => {
	 		var articles:Array[String] = Array()
			if (recommendationsUser._2 != null && recommendationsUser._2.length > 0) {
				articles = recommendationsUser._2.split(",")
			}

			if (articles == null) {
				articles = Array[String]()
			}
			val keyArray = recommendationsUser._1.split("/")
			val userId = keyArray(keyArray.length - 1)
			var articlesRecommended:Array[String] = articles.filter(_ != "null")
			(userId, articlesRecommended)
 		})

 		recommendations
 	}

	def getSystemMilliseconds (dateInput: String): Long = {
		val knownPatterns = new Array[SimpleDateFormat](2)
		knownPatterns(0) = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		knownPatterns(1) = new SimpleDateFormat("MM/dd/yyyy");
		knownPatterns(1) = new SimpleDateFormat("MM/dd/yy");
		
		var dateString = 0L;
		knownPatterns.foreach(pattern => {
		    try {
		    	if(dateString == 0L) {
		        	val date = pattern.parse(dateInput).getTime()
		        	dateString = date
		        }

		   } catch {
		      case e: Exception => {
		        Pretty.outputError(e.getMessage)
      			}
      		}
      	});

		
		if(dateString == 0) {
			return 0L;
		} else {
			return dateString
		}
	}

	def getArticleIdDateMapX(sparkContext: SparkContext, tenantName:String) : RDD[Tuple2[String, Long]] = {
		val pageFiles = sparkContext.wholeTextFiles(String.format("s3n://datomata.%s.articlecontent/*", tenantName).toLowerCase)
		val pageHits:RDD[Tuple2[String, Long]] = pageFiles.map(splitRecord => {
				val json = parse(splitRecord._2)
				val dateString = getSystemMilliseconds(compact(json \\ "createdDate"))
				val pair = (compact(json \\ "articleId"), dateString)
				pair
			}
		)
		pageHits 
	}
		

 	def getArticleIdDateMap(sparkContext: SparkContext, tenantName:String) : RDD[Tuple2[String, Long]] = {
 		setupHadoopConfiguration(sparkContext)
 		
 		val pageFiles = sparkContext.textFile(String.format("s3n://datomata.%s.similararticles/similar_article_recommendations.json", tenantName).toLowerCase)
		val pageHits:RDD[Tuple2[String, Long]] = pageFiles.flatMap(splitRecord => {
			val entries = splitRecord.split("\\}")
			val pairs = new ListBuffer[Tuple2[String, Long]]()
			entries.foreach(entry => {
				val startIndex = entry.indexOf("{")
				if(startIndex != -1) {
					try {
						val finalEntry = String.format("%s}", entry.substring(startIndex)).replace("'", "\"").replace("u", "")
						val json = parse(finalEntry)
						val pair = (compact(json \\ "article").replace("\"", ""), (compact(json \\ "createdDate").replace("\"", "")).toLong)
    					pairs += pair
    				} catch {
						case e: Exception => {
						Pretty.outputError(e.getMessage)
						}
		      		}
				}
			})
			pairs
		});

		pageHits
 	}
 	
 	def getArticleIdSimiliars(sparkContext: SparkContext, tenantName:String) : RDD[Tuple2[String, String]] = {
 		setupHadoopConfiguration(sparkContext)
 		
 		val pageFiles = sparkContext.textFile(String.format("s3n://datomata.%s.similararticles/similar_article_recommendations.json", tenantName).toLowerCase)
		val pageHitsRaw:RDD[Tuple2[String, Tuple2[String, Long]]] = pageFiles.flatMap(splitRecord => {
			val entries = splitRecord.split("\\}")
			val pairs = new ListBuffer[Tuple2[String, Tuple2[String, Long]]]()
			entries.foreach(entry => {
				val startIndex = entry.indexOf("{")
				if(startIndex != -1) {
					try {
					val finalEntry = String.format("%s}", entry.substring(startIndex)).replace("'", "\"").replace("u", "")
					val json = parse(finalEntry)
					var updatedDate = 0L
					try {
						updatedDate = (compact(json \\ "updatedDate").replace("\"", "")).toLong
					} catch {
						case e: Exception => {
						Pretty.outputError(e.getMessage)
						}
					}
					val pair = (compact(json \\ "article").replace("\"", ""), (compact(json \\ "similarArticles").replace("\"", ""), updatedDate))
    				pairs += pair
    				} catch {
						case e: Exception => {
						Pretty.outputError(e.getMessage)
						}
		      		}
				}
			})
			pairs
		});

		val pageHitsFiltered:RDD[Tuple2[String, Tuple2[String, Long]]]  = pageHitsRaw.reduceByKey((a, b) => {
			if(a._2 > b._2) a else b
		})
		
		val pageHits = pageHitsFiltered.map(entry => {
			(entry._1, entry._2._1)
		})

		Pretty.outputString(String.format("%s no of article date map entries", pageHits.count.toString))
		pageHits
 	}
 	def readData(sparkContext: SparkContext, bucketString: String) : RDD[String] = {
 		setupHadoopConfiguration(sparkContext)
 		val pageFiles = sparkContext.wholeTextFiles(bucketString)
		val splitRecords = pageFiles.mapValues(record => {record.split("\\}")}).cache()

		val pageHits = splitRecords.flatMap(splitRecord => {
			val result = mutable.ArrayBuffer[String]()

			splitRecord._2.foreach(splitVal => {
				if(splitVal.length > 0) {
					result += splitVal + "}"
				}
			});
			result
		});

		pageHits;
 	}

 	def readDataV2(sparkContext: SparkContext, bucketString: String) : RDD[String] = {
 		setupHadoopConfiguration(sparkContext)
 		Pretty.outputString(String.format("%s, bucket name for v2 read", bucketString))
 		val pageHits = sparkContext.textFile(bucketString)

 		if (Pretty.isTestLoad() == true) {
 			pageHits.foreach(entry => {
				Pretty.outputString(String.format("readValue - %s", entry))
			})
 		}

		pageHits;
 	}

	def readPipelineConfig(sparkContext: SparkContext, tenantPipeline: String) : scala.collection.Map[String, String] = {
		setupHadoopConfiguration(sparkContext)
		//Pretty.outputString("Reading configuration from S3 for the passed pipeline")
 		
 		//create the tenant config bucket path 
 		val tenantConfigBucket = String.format("s3n://datomata.kiwi.tenants/%s/Pipeline.json", tenantPipeline)
		readConfig(sparkContext, tenantConfigBucket)
	}

	def readGraphConfig(sparkContext: SparkContext, tenantPipeline: String) : scala.collection.Map[String, String] = {
		setupHadoopConfiguration(sparkContext)
 		
 		//create the tenant config bucket path 
 		val tenantConfigBucket = String.format("s3n://datomata.kiwi.tenants/%s/Graph.json", tenantPipeline)
		readConfig(sparkContext, tenantConfigBucket)
	}

	def readSortConfig(sparkContext: SparkContext, tenantPipeline: String) : scala.collection.Map[String, String] = {
		setupHadoopConfiguration(sparkContext)
		//Pretty.outputString("Reading configuration from S3 for the passed pipeline")
 		
 		//create the tenant config bucket path 
 		val tenantConfigBucket = String.format("s3n://datomata.kiwi.tenants/%s/Sort.json", tenantPipeline)
		readConfig(sparkContext, tenantConfigBucket)
	}

	def readTasteConfig(sparkContext: SparkContext, tenantPipeline: String) : scala.collection.Map[String, String] = {
		setupHadoopConfiguration(sparkContext)
		Pretty.outputString("Reading configuration from S3 for the passed pipeline")
 		
 		//create the tenant config bucket path 
 		val tenantConfigBucket = String.format("s3n://datomata.kiwi.tenants/%s/History.json", tenantPipeline)
		readConfig(sparkContext, tenantConfigBucket)
	}

	def readTasteV2Config(sparkContext: SparkContext, tenantPipeline: String) : scala.collection.Map[String, String] = {
		setupHadoopConfiguration(sparkContext)
		Pretty.outputString("Reading configuration from S3 for the passed pipeline")
 		
 		//create the tenant config bucket path 
 		val tenantConfigBucket = String.format("s3n://datomata.kiwi.tenants/%s/HistoryV2.json", tenantPipeline)
		readConfig(sparkContext, tenantConfigBucket)
	}

	def readCountConfig(sparkContext: SparkContext, tenantPipeline: String) : scala.collection.Map[String, String] = {
		setupHadoopConfiguration(sparkContext)
		Pretty.outputString("Reading configuration from S3 for the passed pipeline")
 		
 		//create the tenant config bucket path 
 		val tenantConfigBucket = String.format("s3n://datomata.kiwi.tenants/%s/Count.json", tenantPipeline)
		readConfig(sparkContext, tenantConfigBucket)
	}

	def readAlgorithmConfig(sparkContext: SparkContext, algorithmName: String) : scala.collection.Map[String, String] = {
		setupHadoopConfiguration(sparkContext)
		Pretty.outputString("Reading configuration from S3 for the passed pipeline")
 		
 		//create the tenant config bucket path 
 		val tenantConfigBucket = String.format("s3n://datomata.kiwi.algorithms.config/%s/algorithm.json", algorithmName)
		readConfig(sparkContext, tenantConfigBucket)
	}
 	
 	def readConfig(sparkContext: SparkContext, tenantPipeline: String) : scala.collection.Map[String, String] = {
 		Pretty.outputString(String.format("Reading %s bucket now:", tenantPipeline))
 		val pageFiles = sparkContext.textFile(tenantPipeline)

		Pretty.outputString(String.format("%s entries found in the config", pageFiles.count().toString))
		
		val steps = pageFiles.map(record => {
			Pretty.outputString(String.format("Parsing the configuration record %s", record.toString))
			val json = parse(record)
			Pretty.outputString(String.format("Configuration Record %s", json))
	    	(Pretty.removeQuotes(compact(json \\ "key")), Pretty.removeQuotes(compact(json \\ "value")))
		}).cache()

		val configMap = steps.collectAsMap
		configMap;
 	}
}
